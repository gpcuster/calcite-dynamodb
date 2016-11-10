package calcite.adapter.dynamodb.utils;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

import java.nio.ByteBuffer;

public class DynamoDBTypeConverter {
    public static Class toJavaClass(ScalarAttributeType dynamoDBType) {
        switch (dynamoDBType) {
            case N:
                return Double.class;
            case S:
                return String.class;
            case B:
                return ByteBuffer.class;
            default:
                throw new IllegalArgumentException("Not supported DynamoDB type: " + dynamoDBType);
        }
    }

    public static AttributeValue toDynamoDBAttributeValue(Object obj) {
        AttributeValue attributeValue = new AttributeValue();
        if (obj instanceof String) {
            attributeValue.withS((String) obj);
        } else if (obj instanceof Double
                || obj instanceof Float
                || obj instanceof Long
                || obj instanceof Integer) {
            attributeValue.withN(obj.toString());
        } else if (obj instanceof ByteBuffer) {
            attributeValue.withB((ByteBuffer) obj);
        } else {
            throw new IllegalArgumentException("Not supported java class: " + obj.getClass().toString());
        }

        return attributeValue;
    }
}
