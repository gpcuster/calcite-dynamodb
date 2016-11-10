package calcite.adapter.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class DynamoDBScanEnumerator extends AbstractDynamoDBEnumerator {
    private ScanResult scanResult = null;

    public DynamoDBScanEnumerator(AtomicBoolean cancelFlag, AmazonDynamoDBClient dynamoDBClient, String tableName, Map<String, AttributeValue> dynamoDBTableSchema, String projectionExpression, List<String> filterExpressions, List<String> keyConditionExpressions, Map<String, AttributeValue> expressionAttributeMap) {
        super(cancelFlag, dynamoDBClient, tableName, dynamoDBTableSchema, projectionExpression, filterExpressions, keyConditionExpressions, expressionAttributeMap);
    }

    @Override
    protected void fetchItems() {
        ScanRequest scanRequest = new ScanRequest(tableName);
        if (scanResult != null) {
            scanRequest.setExclusiveStartKey(scanResult.getLastEvaluatedKey());
        }

        if (!StringUtils.isBlank(projectionExpression)) {
            scanRequest.setProjectionExpression(projectionExpression);
        }
        if (!filterExpressions.isEmpty()) {
            String filterExpression = filterExpressions.get(0);
            if (!StringUtils.isBlank(filterExpression)) {
                scanRequest.setFilterExpression(filterExpression);
                scanRequest.setExpressionAttributeValues(expressionAttributeMap);
            }
        }

        scanResult = dynamoDBClient.scan(scanRequest);
        cachedItems = scanResult.getItems();
    }

    @Override
    protected boolean finished() {
        return scanResult.getLastEvaluatedKey() == null || scanResult.getLastEvaluatedKey().isEmpty();
    }

    @Override
    protected void prepareNextFetch() {

    }

    @Override
    protected void resetStatus() {
        scanResult = null;
    }


}
