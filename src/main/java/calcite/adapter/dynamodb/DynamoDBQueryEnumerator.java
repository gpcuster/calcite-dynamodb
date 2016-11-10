package calcite.adapter.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class DynamoDBQueryEnumerator extends AbstractDynamoDBEnumerator {
    private QueryResult queryResult = null;
    private int queryIndex = 0;

    public DynamoDBQueryEnumerator(AtomicBoolean cancelFlag, AmazonDynamoDBClient dynamoDBClient, String tableName, Map<String, AttributeValue> dynamoDBTableSchema, String projectionExpression, List<String> filterExpressions, List<String> keyConditionExpressions, Map<String, AttributeValue> expressionAttributeMap) {
        super(cancelFlag, dynamoDBClient, tableName, dynamoDBTableSchema, projectionExpression, filterExpressions, keyConditionExpressions, expressionAttributeMap);
    }

    @Override
    protected void fetchItems() {
        QueryRequest queryRequest = new QueryRequest(tableName);
        if (queryResult != null) {
            queryRequest.setExclusiveStartKey(queryResult.getLastEvaluatedKey());
        }

        String keyConditionExpression = keyConditionExpressions.get(queryIndex);
        String filterExpression = filterExpressions.get(queryIndex);

        queryRequest.setKeyConditionExpression(keyConditionExpression);

        if (!StringUtils.isBlank(projectionExpression)) {
            queryRequest.setProjectionExpression(projectionExpression);
        }
        if (!StringUtils.isBlank(filterExpression)) {
            queryRequest.setFilterExpression(filterExpression);
        }

        // remove unused expressions.
        Map<String, AttributeValue> trimmedExpressionAttributeMap = new HashMap<>();
        for (String attributeName : expressionAttributeMap.keySet()) {
            if (keyConditionExpression.contains(attributeName) || filterExpression.contains(attributeName)) {
                trimmedExpressionAttributeMap.put(attributeName, expressionAttributeMap.get(attributeName));
            }
        }
        queryRequest.setExpressionAttributeValues(trimmedExpressionAttributeMap);

        queryResult = dynamoDBClient.query(queryRequest);
        cachedItems = queryResult.getItems();
    }

    @Override
    protected boolean finished() {
        return (queryResult.getLastEvaluatedKey() == null || queryResult.getLastEvaluatedKey().isEmpty())
                && queryIndex == keyConditionExpressions.size() - 1;
    }

    @Override
    protected void prepareNextFetch() {
        queryIndex++;
    }

    @Override
    protected void resetStatus() {
        queryIndex = 0;

        queryResult = null;
    }
}
