package calcite.adapter.dynamodb;

import calcite.adapter.dynamodb.tools.AddTableSchema;
import calcite.adapter.dynamodb.tools.CreateMetaTable;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.*;
import org.apache.calcite.util.Util;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.PrintStream;
import java.net.URL;
import java.sql.*;
import java.util.*;
import java.util.function.Function;

/**
 * It will launch a local DynamoDB instance on port 8031.
 */
public class TestDynamoDBAdapter {
    static UnitTestDynamoDBClientHelper helper = new UnitTestDynamoDBClientHelper();
    static String testTableName = "testTable";
    static String metaTableName = "meta";

    static final String EXPLAIN = "explain plan for ";

    static AmazonDynamoDBClient dynamoDBClient;

    @BeforeClass
    public static void before() throws IOException {
        dynamoDBClient = helper.before();

        initMetaTable();
        initTestTable();
    }

    @AfterClass
    public static void after() throws IOException {
        helper.after();
    }

    private static void initMetaTable() {
        CreateMetaTable.createTable(dynamoDBClient, metaTableName);
        Map<String, ScalarAttributeType> tableSchema = new HashMap<>();

        tableSchema.put("hashKey", ScalarAttributeType.S);
        tableSchema.put("sortKey", ScalarAttributeType.S);
        tableSchema.put("stringCol", ScalarAttributeType.S);
        tableSchema.put("numberCol", ScalarAttributeType.N);

        AddTableSchema.addTableSchema(dynamoDBClient, metaTableName, testTableName, tableSchema);
    }

    private static void initTestTable() {
        // create table
        CreateTableRequest createTableRequest = new CreateTableRequest();
        createTableRequest.setTableName(testTableName);
        createTableRequest.setKeySchema(Arrays.asList(new KeySchemaElement("hashKey", KeyType.HASH), new KeySchemaElement("sortKey", KeyType.RANGE)));
        createTableRequest.setAttributeDefinitions(Arrays.asList(new AttributeDefinition("hashKey", ScalarAttributeType.S), new AttributeDefinition("sortKey", ScalarAttributeType.S)));
        createTableRequest.setProvisionedThroughput(new ProvisionedThroughput(10L, 10L));

        dynamoDBClient.createTable(createTableRequest);

        // insert items
        Map<String, List<WriteRequest>> requestItems = new HashMap<>();
        List<WriteRequest> writeRequests = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            PutRequest put = new PutRequest();
            put.addItemEntry("hashKey", new AttributeValue("hashKey" + i));
            put.addItemEntry("sortKey", new AttributeValue("sortKey" + i));
            put.addItemEntry("stringCol", new AttributeValue("stringCol" + i));
            put.addItemEntry("numberCol", new AttributeValue().withN(i + ""));

            writeRequests.add(new WriteRequest(put));
        }
        requestItems.put(testTableName, writeRequests);

        dynamoDBClient.batchWriteItem(new BatchWriteItemRequest().withRequestItems(requestItems));
    }

    private String jsonPath(String model) {
        return resourcePath(model + ".json");
    }

    private String resourcePath(String path) {
        final URL url = TestDynamoDBAdapter.class.getResource("/" + path);

        String s = url.toString();
        if (s.startsWith("file:")) {
            s = s.substring("file:".length());
        }
        return s;
    }

    private void checkSql(String model, String sql) throws SQLException {
        checkSql(sql, model, output());
    }

    private Function<ResultSet, Void> output() {
        return resultSet -> {
            try {
                output(resultSet, System.out);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            return null;
        };
    }

    private void output(ResultSet resultSet, PrintStream out)
            throws SQLException {
        final ResultSetMetaData metaData = resultSet.getMetaData();
        final int columnCount = metaData.getColumnCount();
        while (resultSet.next()) {
            for (int i = 1; ; i++) {
                out.print(resultSet.getString(i));
                if (i < columnCount) {
                    out.print(", ");
                } else {
                    out.println();
                    break;
                }
            }
        }
    }

    private void checkSql(String sql, String model, Function<ResultSet, Void> fn)
            throws SQLException {
        Connection connection = null;
        Statement statement = null;
        try {
            Properties info = new Properties();
            info.put("model", jsonPath(model));
            info.put("unquotedCasing", "UNCHANGED");
            info.put("caseSensitive", "true");
            connection = DriverManager.getConnection("jdbc:calcite:", info);
            statement = connection.createStatement();
            final ResultSet resultSet =
                    statement.executeQuery(
                            sql);
            fn.apply(resultSet);
        } finally {
            close(connection, statement);
        }
    }

    private void close(Connection connection, Statement statement) {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                // ignore
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    private static void collect(List<String> result, ResultSet resultSet)
            throws SQLException {
        final StringBuilder buf = new StringBuilder();
        while (resultSet.next()) {
            buf.setLength(0);
            int n = resultSet.getMetaData().getColumnCount();
            String sep = "";
            for (int i = 1; i <= n; i++) {
                buf.append(sep)
                        .append(resultSet.getMetaData().getColumnLabel(i))
                        .append("=")
                        .append(resultSet.getString(i));
                sep = "; ";
            }
            result.add(Util.toLinux(buf.toString()));
        }
    }

    /**
     * Returns a function that checks the contents of a result set against an
     * expected string.
     */
    private Function<ResultSet, Void> expect(final String... expected) {
        return resultSet -> {
            try {
                final List<String> lines = new ArrayList<>();
                collect(lines, resultSet);
                Assert.assertEquals(Arrays.asList(expected), lines);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            return null;
        };
    }

    private void checkSql(String model, String sql, final String... expected)
            throws SQLException {
        checkSql(sql, model, expect(expected));
    }

    ///---------------

    @Test
    public void testSelectAll() throws SQLException {
        checkSql("testModel",
                "select * from " + testTableName,
                "numberCol=9.0; hashKey=hashKey9; sortKey=sortKey9; stringCol=stringCol9",
                "numberCol=5.0; hashKey=hashKey5; sortKey=sortKey5; stringCol=stringCol5",
                "numberCol=3.0; hashKey=hashKey3; sortKey=sortKey3; stringCol=stringCol3",
                "numberCol=4.0; hashKey=hashKey4; sortKey=sortKey4; stringCol=stringCol4",
                "numberCol=0.0; hashKey=hashKey0; sortKey=sortKey0; stringCol=stringCol0",
                "numberCol=1.0; hashKey=hashKey1; sortKey=sortKey1; stringCol=stringCol1",
                "numberCol=7.0; hashKey=hashKey7; sortKey=sortKey7; stringCol=stringCol7",
                "numberCol=6.0; hashKey=hashKey6; sortKey=sortKey6; stringCol=stringCol6",
                "numberCol=8.0; hashKey=hashKey8; sortKey=sortKey8; stringCol=stringCol8",
                "numberCol=2.0; hashKey=hashKey2; sortKey=sortKey2; stringCol=stringCol2");
    }

    @Test
    public void testSelectOneField() throws SQLException {
        checkSql("testModel",
                "select hashKey from " + testTableName,
                "hashKey=hashKey9",
                "hashKey=hashKey5",
                "hashKey=hashKey3",
                "hashKey=hashKey4",
                "hashKey=hashKey0",
                "hashKey=hashKey1",
                "hashKey=hashKey7",
                "hashKey=hashKey6",
                "hashKey=hashKey8",
                "hashKey=hashKey2");
    }

    @Test
    public void testSelectTwoFields() throws SQLException {
        checkSql("testModel",
                "select hashKey, sortKey from " + testTableName,
                "hashKey=hashKey9; sortKey=sortKey9",
                "hashKey=hashKey5; sortKey=sortKey5",
                "hashKey=hashKey3; sortKey=sortKey3",
                "hashKey=hashKey4; sortKey=sortKey4",
                "hashKey=hashKey0; sortKey=sortKey0",
                "hashKey=hashKey1; sortKey=sortKey1",
                "hashKey=hashKey7; sortKey=sortKey7",
                "hashKey=hashKey6; sortKey=sortKey6",
                "hashKey=hashKey8; sortKey=sortKey8",
                "hashKey=hashKey2; sortKey=sortKey2");
    }

    @Test
    public void testFilterSortKey() throws SQLException {
        String sql = "select hashKey, sortKey from " + testTableName
                + " where sortKey >= 'sortKey1' and sortKey <= 'sortKey3'";

        checkSql("testModel",
                sql,
                "hashKey=hashKey3; sortKey=sortKey3",
                "hashKey=hashKey1; sortKey=sortKey1",
                "hashKey=hashKey2; sortKey=sortKey2");
    }

    @Test
    public void testFilterHashKey() throws SQLException {
        String sql = "select hashKey, sortKey from " + testTableName
                + " where hashKey = 'hashKey1'"
                + " or hashKey = 'hashKey2'";

        checkSql("testModel",
                sql,
                "hashKey=hashKey1; sortKey=sortKey1",
                "hashKey=hashKey2; sortKey=sortKey2");
    }

    @Test
    public void testFilterHashKeyAndSortKey() throws SQLException {
        String sql = "select hashKey, sortKey from " + testTableName
                + " where hashKey = 'hashKey1'"
                + " or (hashKey = 'hashKey2' and sortKey >= 'sortKey1' and sortKey <= 'sortKey9')";

        checkSql("testModel",
                sql,
                "hashKey=hashKey1; sortKey=sortKey1",
                "hashKey=hashKey2; sortKey=sortKey2");
    }

    @Test
    public void testFilterNumberCol() throws SQLException {
        String sql = "select numberCol from " + testTableName
                + " where numberCol > 8";

        checkSql("testModel",
                sql,
                "numberCol=9.0");
    }


}
