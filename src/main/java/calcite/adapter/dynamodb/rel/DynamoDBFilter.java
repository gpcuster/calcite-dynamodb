package calcite.adapter.dynamodb.rel;

import com.google.common.collect.Iterators;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.commons.lang.StringUtils;

import java.util.*;
import java.util.function.Function;

public class DynamoDBFilter extends Filter implements DynamoDBRel {
    private final String hashKeyName;
    private final String sortKeyName;

    public DynamoDBFilter(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode child,
            RexNode condition,
            String hashKeyName,
            String sortKeyName) {
        super(cluster, traitSet, child, condition);

        this.hashKeyName = hashKeyName;
        this.sortKeyName = sortKeyName;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy(0.1);
    }

    public DynamoDBFilter copy(RelTraitSet traitSet, RelNode input,
                               RexNode condition) {
        return new DynamoDBFilter(getCluster(), traitSet, input, condition, hashKeyName, sortKeyName);
    }

    private String generateFilterExpression(List<Translator.AndFilters> orFilters, Function<Translator.AndFilters, Iterator<Translator.Filter>> getFilterIter) {
        StringBuilder orExpressionSb = new StringBuilder();
        for (Translator.AndFilters andFilers : orFilters) {
            StringBuilder addFilerExpressionSb = new StringBuilder();
            Iterator<Translator.Filter> filterIter = getFilterIter.apply(andFilers);
            while (filterIter.hasNext()) {
                Translator.Filter filter = filterIter.next();
                if (filter != null) {
                    if (addFilerExpressionSb.length() > 0) {
                        addFilerExpressionSb.append(" and ");
                    }

                    addFilerExpressionSb.append(filter.name);
                    addFilerExpressionSb.append(" ");
                    addFilerExpressionSb.append(filter.op);
                    addFilerExpressionSb.append(" ");
                    addFilerExpressionSb.append(filter.expressionAttributeName);
                }
            }
            String addFilerExpression = addFilerExpressionSb.toString();

            if (!StringUtils.isBlank(addFilerExpression)) {
                if (orExpressionSb.length() > 0) {
                    orExpressionSb.append(" or ");
                }

                orExpressionSb.append("(");
                orExpressionSb.append(addFilerExpression);
                orExpressionSb.append(")");
            }
        }

        return orExpressionSb.toString();
    }

    private String generateKeyConditionExpression(Translator.Filter hashKeyFilter, List<Translator.Filter> sortKeyFilters) {
        StringBuilder keyConditionExpressionSb = new StringBuilder();
        keyConditionExpressionSb.append(hashKeyFilter.name);
        keyConditionExpressionSb.append(" ");
        keyConditionExpressionSb.append(hashKeyFilter.op);
        keyConditionExpressionSb.append(" ");
        keyConditionExpressionSb.append(hashKeyFilter.expressionAttributeName);

        if (sortKeyFilters.size() == 1) {
            keyConditionExpressionSb.append(" and ");

            keyConditionExpressionSb.append(sortKeyFilters.get(0).name);
            keyConditionExpressionSb.append(" ");
            keyConditionExpressionSb.append(sortKeyFilters.get(0).op);
            keyConditionExpressionSb.append(" ");
            keyConditionExpressionSb.append(sortKeyFilters.get(0).expressionAttributeName);
        } else if (sortKeyFilters.size() == 2) {
            // convert into BETWEEN
            keyConditionExpressionSb.append(" and ");

            keyConditionExpressionSb.append(sortKeyFilters.get(0).name);
            keyConditionExpressionSb.append(" BETWEEN ");
            keyConditionExpressionSb.append(sortKeyFilters.get(0).expressionAttributeName);
            keyConditionExpressionSb.append(" AND ");
            keyConditionExpressionSb.append(sortKeyFilters.get(1).expressionAttributeName);
        }

        return keyConditionExpressionSb.toString();
    }

    @Override
    public void implement(Implementor implementor) {
        implementor.visitChild(0, getInput());

        List<String> fieldNames = new ArrayList<>();
        for (RelDataTypeField fieldType : getRowType().getFieldList()) {
            fieldNames.add(fieldType.getKey());
        }

        Translator translator = new Translator(fieldNames, hashKeyName, sortKeyName);
        Translator.Result result = translator.translateCondition(condition);

        // convert map into 2 lists, so it's easy to pass them into compiled code afterwards.
        for (Map.Entry<String, Object> expressionAttribute : result.expressionAttributeValues.entrySet()) {
            implementor.addExpressionAttributeName(expressionAttribute.getKey());
            implementor.addExpressionAttributeVaule(expressionAttribute.getValue());
        }

        if (result.hashKeyFilterCount == result.filters.size()) {
            // all filters contain hashKey filter, so we can convert it into multiple queries.
            for (Translator.AndFilters filter : result.filters) {
                String keyConditionExpression = generateKeyConditionExpression(filter.hashKeyFilter, filter.sortKeyFilters);
                implementor.addKeyConditionExpression(keyConditionExpression);

                String filerExpression = generateFilterExpression(Arrays.asList(filter), andFilers -> andFilers.otherFilters.iterator());
                implementor.addFilterExpression(filerExpression);
            }
        } else {
            // convert into scan.
            String filerExpression = generateFilterExpression(result.filters,
                    andFilers -> Iterators.concat(
                            Iterators.concat(andFilers.otherFilters.iterator(), Arrays.asList(andFilers.hashKeyFilter).iterator()),
                            andFilers.sortKeyFilters.iterator())
            );
            implementor.addFilterExpression(filerExpression);
        }
    }

    /**
     * Translates {@link RexNode} expressions into DynamoDB expression strings.
     */
    static class Translator {
        static final List<String> HASHKEY_SUPPORTED_OP = Arrays.asList("=");
        static final List<String> SORTKEY_SUPPORTED_OP = Arrays.asList("=", ">", ">=", "<", "<=");

        static class Result {
            List<AndFilters> filters;
            Map<String, Object> expressionAttributeValues;
            int hashKeyFilterCount;
        }

        static class Filter {
            String op;
            String name;
            String expressionAttributeName;

            public Filter(String op, String name, String expressionAttributeName) {
                this.op = op;
                this.name = name;
                this.expressionAttributeName = expressionAttributeName;
            }
        }

        static class AndFilters {
            Filter hashKeyFilter;
            List<Filter> sortKeyFilters = new ArrayList<>();
            List<Filter> otherFilters = new ArrayList<>();
        }

        private AndFilters andFilters;
        private Map<String, Object> expressionAttributeValues = new HashMap<>();
        private int valCounter = 1;

        private final List<String> fieldNames;
        private final String hashKeyName;
        private final String sortKeyName;

        Translator(List<String> fieldNames, String hashKeyName, String sortKeyName) {
            this.fieldNames = fieldNames;
            this.hashKeyName = hashKeyName;
            this.sortKeyName = sortKeyName;
        }

        private Result translateCondition(RexNode condition) {
            Result result = new Result();

            result.filters = translateOr(condition);
            result.expressionAttributeValues = expressionAttributeValues;
            int hashKeyFilterCount = 0;
            for (AndFilters addFilter : result.filters) {
                if (addFilter.hashKeyFilter != null) {
                    hashKeyFilterCount++;
                }
            }
            result.hashKeyFilterCount = hashKeyFilterCount;

            return result;
        }

        private List<AndFilters> translateOr(RexNode condition) {
            List<AndFilters> filters = new ArrayList<>();
            for (RexNode node : RelOptUtil.disjunctions(condition)) {
                filters.add(translateAnd(node));
            }
            return filters;
        }

        /**
         * Translates a condition that may be an AND of other conditions. Gathers
         * together conditions that apply to the same field.
         */
        private AndFilters translateAnd(RexNode node0) {
            andFilters = new AndFilters();
            for (RexNode node : RelOptUtil.conjunctions(node0)) {
                translateMatch2(node);
            }

            return andFilters;
        }

        private static Object literalValue(RexLiteral literal) {
            return literal.getValue2();
        }

        private Void translateMatch2(RexNode node) {
            switch (node.getKind()) {
                case EQUALS:
                    return translateBinary("=", "=", (RexCall) node);
                case LESS_THAN:
                    return translateBinary("<", ">", (RexCall) node);
                case LESS_THAN_OR_EQUAL:
                    return translateBinary("<=", ">=", (RexCall) node);
                case NOT_EQUALS:
                    return translateBinary("<>", "<>", (RexCall) node);
                case GREATER_THAN:
                    return translateBinary(">", "<", (RexCall) node);
                case GREATER_THAN_OR_EQUAL:
                    return translateBinary(">=", "<=", (RexCall) node);
                // TODO: add other dynamoDB supported kinds.
                default:
                    throw new AssertionError("cannot translate " + node);
            }
        }

        /**
         * Translates a call to a binary operator, reversing arguments if
         * necessary.
         */
        private Void translateBinary(String op, String rop, RexCall call) {
            final RexNode left = call.operands.get(0);
            final RexNode right = call.operands.get(1);
            boolean b = translateBinary2(op, left, right);
            if (b) {
                return null;
            }
            b = translateBinary2(rop, right, left);
            if (b) {
                return null;
            }
            throw new AssertionError("cannot translate op " + op + " call " + call);
        }

        /**
         * Translates a call to a binary operator. Returns whether successful.
         */
        private boolean translateBinary2(String op, RexNode left, RexNode right) {
            switch (right.getKind()) {
                case LITERAL:
                    break;
                default:
                    return false;
            }
            final RexLiteral rightLiteral = (RexLiteral) right;
            switch (left.getKind()) {
                case INPUT_REF:
                    final RexInputRef left1 = (RexInputRef) left;
                    String name = fieldNames.get(left1.getIndex());
                    translateOp2(op, name, rightLiteral);
                    return true;
                case CAST:
                    return translateBinary2(op, ((RexCall) left).operands.get(0), right);
                case OTHER_FUNCTION:
                    String itemName = isItem((RexCall) left);
                    if (itemName != null) {
                        translateOp2(op, itemName, rightLiteral);
                        return true;
                    }
                    // fall through
                default:
                    return false;
            }
        }

        private void translateOp2(String op, String name, RexLiteral right) {
            String expressionAttributeName = ":val" + valCounter + "_" + name;
            valCounter++;

            expressionAttributeValues.put(expressionAttributeName, literalValue(right));

            if (hashKeyName.equals(name) && HASHKEY_SUPPORTED_OP.contains(op)) {
                if (andFilters.hashKeyFilter != null) {
                    throw new IllegalArgumentException("only allow one condition for hash key: " + hashKeyName + " in each where or clause");
                }
                andFilters.hashKeyFilter = new Filter(op, name, expressionAttributeName);
            } else if (sortKeyName != null && sortKeyName.equals(name) && SORTKEY_SUPPORTED_OP.contains(op)) {
                if (andFilters.sortKeyFilters.isEmpty()) {
                    andFilters.sortKeyFilters.add(new Filter(op, name, expressionAttributeName));
                } else {
                    Filter existingSortKeyFilter = andFilters.sortKeyFilters.get(0);
                    if (andFilters.sortKeyFilters.size() == 1
                            && (existingSortKeyFilter.op == ">=" && op == "<=")
                            || (existingSortKeyFilter.op == "<=" && op == ">=")) {
                        andFilters.sortKeyFilters.add(new Filter(op, name, expressionAttributeName));
                    } else {
                        throw new IllegalArgumentException("only allow one condition or between condition for sort key: " + sortKeyName + " in each where or clause");
                    }
                }
            } else {
                andFilters.otherFilters.add(new Filter(op, name, expressionAttributeName));
            }
        }

        /**
         * Returns 'string' if it is a call to item['string'], null otherwise.
         */
        static String isItem(RexCall call) {
            if (call.getOperator() != SqlStdOperatorTable.ITEM) {
                return null;
            }
            final RexNode op0 = call.operands.get(0);
            final RexNode op1 = call.operands.get(1);
            if (op0 instanceof RexInputRef
                    && ((RexInputRef) op0).getIndex() == 0
                    && op1 instanceof RexLiteral
                    && ((RexLiteral) op1).getValue2() instanceof String) {
                return (String) ((RexLiteral) op1).getValue2();
            }
            return null;
        }
    }
}