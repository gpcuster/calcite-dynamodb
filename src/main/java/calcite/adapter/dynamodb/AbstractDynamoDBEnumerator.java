package calcite.adapter.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import org.apache.calcite.linq4j.Enumerator;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractDynamoDBEnumerator implements Enumerator<Object> {
    protected AmazonDynamoDBClient dynamoDBClient;
    private AtomicBoolean cancelFlag;
    private Map<String, AttributeValue> dynamoDBTableSchema;

    protected String tableName;
    protected String projectionExpression;
    protected List<String> filterExpressions;
    protected List<String> keyConditionExpressions;
    protected Map<String, AttributeValue> expressionAttributeMap;

    protected List<Map<String, AttributeValue>> cachedItems = null;

    private int currentItemIndex = -1;

    public AbstractDynamoDBEnumerator(AtomicBoolean cancelFlag, AmazonDynamoDBClient dynamoDBClient, String tableName, Map<String, AttributeValue> dynamoDBTableSchema,
                                      String projectionExpression, List<String> filterExpressions, List<String> keyConditionExpressions, Map<String, AttributeValue> expressionAttributeMap) {
        this.cancelFlag = cancelFlag;
        this.dynamoDBClient = dynamoDBClient;
        this.tableName = tableName;
        this.dynamoDBTableSchema = dynamoDBTableSchema;

        this.projectionExpression = projectionExpression;
        this.filterExpressions = filterExpressions;
        this.keyConditionExpressions = keyConditionExpressions;
        this.expressionAttributeMap = expressionAttributeMap;

        reset();
    }

    protected abstract void fetchItems();

    protected abstract boolean finished();

    protected abstract void prepareNextFetch();

    protected abstract void resetStatus();

    public Map<String, AttributeValue> next() {
        if (currentItemIndex < cachedItems.size()) {
            return cachedItems.get(currentItemIndex);
        } else if (!finished()) {
            currentItemIndex = 0;

            prepareNextFetch();
            fetchItems();

            return next();
        }

        return null;
    }

    @Override
    public Object current() {
        return convertRow(next());
    }

    @Override
    public boolean moveNext() {
        if (cancelFlag.get()) {
            return false;
        }

        if (currentItemIndex == -1) {
            fetchItems();
        }

        currentItemIndex++;

        Map<String, AttributeValue> nextItem = next();
        if (nextItem == null) {
            return false;
        } else {
            return true;
        }
    }

    @Override
    public void reset() {
        currentItemIndex = -1;

        resetStatus();
    }

    @Override
    public void close() {

    }

    private Object convertRow(Map<String, AttributeValue> row) {
        final Object[] objects = new Object[row.size()];

        int index = 0;
        for (Map.Entry<String, AttributeValue> rowField : row.entrySet()) {
            String filedName = rowField.getKey();
            ScalarAttributeType fieldType = ScalarAttributeType.valueOf(dynamoDBTableSchema.get(filedName).getS());

            Object filedValue;
            AttributeValue attributeValue = row.get(filedName);

            switch (fieldType) {
                case N:
                    filedValue = Double.valueOf(attributeValue.getN());
                    break;
                case S:
                    filedValue = attributeValue.getS();
                    break;
                case B:
                    filedValue = attributeValue.getB();
                    break;
                default:
                    throw new IllegalArgumentException("Not supported DynamoDB type: " + fieldType + " for attribute: " + filedName);
            }

            objects[index] = filedValue;

            index++;
        }

        if (objects.length == 1) {
            return objects[0];
        } else {
            return objects;
        }
    }
}
