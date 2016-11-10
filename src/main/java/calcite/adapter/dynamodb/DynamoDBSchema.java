package calcite.adapter.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.*;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.util.trace.CalciteTrace;
import org.slf4j.Logger;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

public class DynamoDBSchema extends AbstractSchema {
    private static final Logger LOGGER = CalciteTrace.getPlannerTracer();

    public static final String META_TABLE_NAME = "TABLE_NAME";

    private AmazonDynamoDBClient dynamoDBClient;
    private String metaTableName;

    public DynamoDBSchema(AmazonDynamoDBClient dynamoDBClient, String metaTableName) {
        this.dynamoDBClient = dynamoDBClient;
        this.metaTableName = metaTableName;
    }

    private Iterator<Map<String, AttributeValue>> getDynamoDBTableSchemas() {
        return new Iterator<Map<String, AttributeValue>>() {
            int currentIndex = 0;
            ScanResult scanResult = null;

            private boolean scanFinished() {
                return scanResult.getLastEvaluatedKey() == null || scanResult.getLastEvaluatedKey().isEmpty();
            }

            private void scan() {
                ScanRequest scanRequest = new ScanRequest(metaTableName);
                if (scanResult != null) {
                    scanRequest.setExclusiveStartKey(scanResult.getLastEvaluatedKey());
                }

                scanResult = dynamoDBClient.scan(scanRequest);
            }

            @Override
            public boolean hasNext() {
                if (scanResult == null) {
                    scan();
                }

                return currentIndex < scanResult.getItems().size() || !scanFinished();
            }

            @Override
            public Map<String, AttributeValue> next() {
                if (currentIndex < scanResult.getItems().size()) {
                    Map<String, AttributeValue> item = scanResult.getItems().get(currentIndex);

                    currentIndex++;

                    return item;
                } else if (!scanFinished()) {
                    scan();
                    currentIndex = 0;

                    return next();
                }

                throw new NoSuchElementException("Not more element to return.");
            }
        };
    }

    private DynamoDBTable createTable(Map<String, AttributeValue> dynamoDBTableSchema,
                                      String tableName, String hashKeyName, String sortKeyName) {
        return new DynamoDBTable(dynamoDBTableSchema, tableName, hashKeyName, sortKeyName, dynamoDBClient);
    }

    @Override
    protected Map<String, Table> getTableMap() {
        final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();

        Iterator<Map<String, AttributeValue>> dynamoDBTableSchemas = getDynamoDBTableSchemas();

        while (dynamoDBTableSchemas.hasNext()) {
            Map<String, AttributeValue> dynamoDBTableSchema = dynamoDBTableSchemas.next();

            String tableName = dynamoDBTableSchema.get(META_TABLE_NAME).getS();

            TableDescription tableDescription = dynamoDBClient.describeTable(tableName).getTable();

            if (tableDescription.getTableStatus().equals("ACTIVE")) {
                dynamoDBTableSchema.remove(META_TABLE_NAME);

                String hashKeyName = null;
                String sortKeyName = null;
                for (KeySchemaElement keySchemaElement : tableDescription.getKeySchema()) {
                    if (keySchemaElement.getKeyType().equals("HASH")) {
                        hashKeyName = keySchemaElement.getAttributeName();
                    } else if (keySchemaElement.getKeyType().equals("RANGE")) {
                        sortKeyName = keySchemaElement.getAttributeName();
                    }
                }

                DynamoDBTable table = createTable(dynamoDBTableSchema, tableName, hashKeyName, sortKeyName);

                builder.put(tableName, table);
            } else {
                LOGGER.warn("Ignoring table " + tableName + " as the status is: " + tableDescription.getTableStatus());
            }


        }

        return builder.build();
    }
}

