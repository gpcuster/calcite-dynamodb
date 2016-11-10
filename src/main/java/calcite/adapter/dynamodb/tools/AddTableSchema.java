package calcite.adapter.dynamodb.tools;

import calcite.adapter.dynamodb.utils.DynamoDBClientUtil;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.*;

import java.util.HashMap;
import java.util.Map;

public class AddTableSchema {
    public static void main(String[] args) {
        String regionName = "us-west-2";
        String metaTableName = "META";

        AmazonDynamoDBClient client = DynamoDBClientUtil.createAmazonDynamoDBClient(regionName, false);

        String tableName = "SALES";
        Map<String, ScalarAttributeType> tableSchema = new HashMap<>();
        tableSchema.put("id", ScalarAttributeType.S);
        tableSchema.put("f1S", ScalarAttributeType.S);
        tableSchema.put("f2N", ScalarAttributeType.N);
        tableSchema.put("f3B", ScalarAttributeType.B);

        addTableSchema(client, metaTableName, tableName, tableSchema);
    }

    public static void addTableSchema(AmazonDynamoDBClient client, String metaTableName, String tableName, Map<String, ScalarAttributeType> tableSchema) {
        PutItemRequest putItemRequest = new PutItemRequest();
        putItemRequest.setTableName(metaTableName);

        Map<String, AttributeValue> newItem = new HashMap<>();
        newItem.put("TABLE_NAME", new AttributeValue(tableName));
        for (Map.Entry<String, ScalarAttributeType> filed : tableSchema.entrySet()) {
            newItem.put(filed.getKey(), new AttributeValue(filed.getValue().toString()));
        }

        putItemRequest.setReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        putItemRequest.setReturnItemCollectionMetrics("SIZE");

        putItemRequest.setItem(newItem);
        PutItemResult ret = client.putItem(putItemRequest);
        System.out.println(ret);
    }
}
