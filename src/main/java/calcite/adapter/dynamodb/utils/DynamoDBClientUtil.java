package calcite.adapter.dynamodb.utils;

import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;

public class DynamoDBClientUtil {
    private static String localEndPoint = "http://localhost:8031";

    public static AmazonDynamoDBClient createAmazonDynamoDBClient(String regionName, boolean local) {
        AmazonDynamoDBClient amazonDynamoDBClient = new AmazonDynamoDBClient();

        if (local) {
            amazonDynamoDBClient.setEndpoint(localEndPoint);
        } else {
            amazonDynamoDBClient.setRegion(RegionUtils.getRegion(regionName));
        }

        return amazonDynamoDBClient;
    }
}
