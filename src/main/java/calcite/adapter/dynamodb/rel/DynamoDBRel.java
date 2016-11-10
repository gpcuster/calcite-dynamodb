package calcite.adapter.dynamodb.rel;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Relational expression that uses DynamoDB calling convention.
 */
public interface DynamoDBRel extends RelNode {
    void implement(Implementor implementor);

    /**
     * Calling convention for relational operations that occur in DynamoDB.
     */
    Convention CONVENTION = new Convention.Impl("DynamoDB", DynamoDBRel.class);

    /**
     * Callback for the implementation process that converts a tree of
     * {@link DynamoDBRel} nodes into a MongoDB query.
     */
    class Implementor {
        private final List<String> attributesToGet = new ArrayList<>();
        private final List<String> filterExpressions = new ArrayList<>();
        private final List<String> keyConditionExpressions = new ArrayList<>();
        private final List<String> expressionAttributeNames = new ArrayList<>();
        private final List<Object> expressionAttributeValues = new ArrayList<>();

        RelOptTable table;

        public void addAttributeToGet(String attributeToGet) {
            attributesToGet.add(attributeToGet);
        }

        public void addFilterExpression(String filterExpression) {
            filterExpressions.add(filterExpression);
        }

        public List<String> getFilterExpressions() {
            return filterExpressions;
        }

        public void addKeyConditionExpression(String keyConditionExpression) {
            this.keyConditionExpressions.add(keyConditionExpression);
        }

        public List<String> getKeyConditionExpressions() {
            return keyConditionExpressions;
        }

        public String getProjectionExpression() {
            StringBuilder sb = new StringBuilder();
            for (String attribute : attributesToGet) {
                if (sb.length() > 0) {
                    sb.append(", ");
                }
                sb.append(attribute);
            }

            return sb.toString();
        }

        public void addExpressionAttributeName(String expressionAttributeName) {
            expressionAttributeNames.add(expressionAttributeName);
        }

        public void addExpressionAttributeVaule(Object expressionAttributeVaule) {
            expressionAttributeValues.add(expressionAttributeVaule);
        }

        public List<String> getExpressionAttributeNames() {
            return expressionAttributeNames;
        }

        public List<Object> getExpressionAttributeValues() {
            return expressionAttributeValues;
        }

        public void visitChild(int ordinal, RelNode input) {
            assert ordinal == 0;
            ((DynamoDBRel) input).implement(this);
        }
    }
}