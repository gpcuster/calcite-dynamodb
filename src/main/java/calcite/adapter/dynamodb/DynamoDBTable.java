package calcite.adapter.dynamodb;

import calcite.adapter.dynamodb.rel.DynamoDBRel;
import calcite.adapter.dynamodb.rel.DynamoDBTableScan;
import calcite.adapter.dynamodb.utils.DynamoDBTypeConverter;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.*;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.Pair;

import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class DynamoDBTable extends AbstractTable implements QueryableTable, TranslatableTable {
    private Map<String, AttributeValue> dynamoDBTableSchema;
    private String tableName;
    private AmazonDynamoDBClient dynamoDBClient;
    private String hashKeyName;
    private String sortKeyName;

    public DynamoDBTable(Map<String, AttributeValue> dynamoDBTableSchema, String tableName, String hashKeyName, String sortKeyName, AmazonDynamoDBClient dynamoDBClient) {
        this.dynamoDBTableSchema = dynamoDBTableSchema;
        this.tableName = tableName;
        this.hashKeyName = hashKeyName;
        this.sortKeyName = sortKeyName;
        this.dynamoDBClient = dynamoDBClient;
    }

    /**
     * <p>Called from generated code.
     */
    public Enumerable<Object> scanOrQuery(final DataContext root, final String projectionExpression,
                                          final List<String> filterExpressions, final List<String> expressionAttributeNames,
                                          final List<Object> expressionAttributeValues, final List<String> keyConditionExpressions) {
        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);

        Map<String, AttributeValue> expressionAttributeMap = new HashMap<>();
        for (int i = 0; i < expressionAttributeNames.size(); i++) {
            expressionAttributeMap.put(expressionAttributeNames.get(i), DynamoDBTypeConverter.toDynamoDBAttributeValue(expressionAttributeValues.get(i)));
        }

        final boolean scan = keyConditionExpressions.isEmpty();

        return new AbstractEnumerable<Object>() {
            public Enumerator<Object> enumerator() {
                if (scan) {
                    return new DynamoDBScanEnumerator(cancelFlag, dynamoDBClient, tableName, dynamoDBTableSchema, projectionExpression, filterExpressions, keyConditionExpressions,
                            expressionAttributeMap);
                } else {
                    return new DynamoDBQueryEnumerator(cancelFlag, dynamoDBClient, tableName, dynamoDBTableSchema, projectionExpression, filterExpressions, keyConditionExpressions,
                            expressionAttributeMap);
                }
            }
        };
    }

    public Expression getExpression(SchemaPlus schema, String tableName, Class clazz) {
        return Schemas.tableExpression(schema, getElementType(), tableName, clazz);
    }

    @Override
    public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
        throw new UnsupportedOperationException();
    }

    public Type getElementType() {
        return Object[].class;
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        final RelOptCluster cluster = context.getCluster();
        return new DynamoDBTableScan(context.getCluster(), cluster.traitSetOf(DynamoDBRel.CONVENTION), relOptTable, this, relOptTable.getRowType(),
                hashKeyName, sortKeyName);
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        final List<RelDataType> types = new ArrayList<>();
        final List<String> names = new ArrayList<>();

        for (Map.Entry<String, AttributeValue> fieldSchema : dynamoDBTableSchema.entrySet()) {
            String name = fieldSchema.getKey();
            ScalarAttributeType type = ScalarAttributeType.valueOf(fieldSchema.getValue().getS());

            switch (type) {
                case N:
                    types.add(typeFactory.createJavaType(Double.class));
                    break;
                case S:
                    types.add(typeFactory.createJavaType(String.class));
                    break;
                case B:
                    types.add(typeFactory.createJavaType(ByteBuffer.class));
                    break;
                default:
                    throw new IllegalArgumentException("Not supported DynamoDB type: " + type + " for attribute: " + name);
            }
            names.add(name);
        }

        return typeFactory.createStructType(Pair.zip(names, types));
    }
}
