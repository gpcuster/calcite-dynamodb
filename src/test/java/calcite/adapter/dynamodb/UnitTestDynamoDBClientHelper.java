package calcite.adapter.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * This is a unit test helper class to get a clean local in memory DynamoDB Client.
 */
public class UnitTestDynamoDBClientHelper {
    final static Logger LOGGER = Logger.getLogger(UnitTestDynamoDBClientHelper.class);

    // TODO: Running on docker to avoid multiple instance share the same port issue.
    public static final String LOCAL_END_POINT = "http://localhost:8031";

    Process dynamoDBLocalProcess;

    /**
     * run this method in the Before unit test method.
     **/
    public AmazonDynamoDBClient before() throws IOException {
        ProcessBuilder pb = new ProcessBuilder("java",
                "-Xmx64m",
                "-Djava.library.path=./dynamodb_local/DynamoDBLocal_lib",
                "-jar", "dynamodb_local/DynamoDBLocal.jar",
                "-inMemory",
                "-port", "8031");
        dynamoDBLocalProcess = pb.start();

        if (!dynamoDBLocalProcess.isAlive()) {
            throw new IOException("start local dynamoDB failed");
        }

        return createDynamoDBClient();
    }

    /**
     * run this method in the After unit test method.
     */
    public void after() throws IOException {
        dynamoDBLocalProcess.destroyForcibly();

        int maxRetry = 10;
        int currentRetry = 0;
        while (currentRetry < maxRetry) {
            if (!dynamoDBLocalProcess.isAlive()) {
                break;
            } else {
                try {
                    LOGGER.info("local DynamoDB still running, sleeping 0.2 sec");
                    Thread.sleep(200);
                    currentRetry++;
                } catch (InterruptedException e) {
                    break;
                }
            }
        }

        if (dynamoDBLocalProcess.isAlive()) {
            throw new IOException("terminate local dynamoDB failed");
        }
    }

    public static AmazonDynamoDBClient createDynamoDBClient() {
        AmazonDynamoDBClient amazonDynamoDBClient = new AmazonDynamoDBClient();
        amazonDynamoDBClient.setEndpoint(LOCAL_END_POINT);

        return amazonDynamoDBClient;
    }
}
