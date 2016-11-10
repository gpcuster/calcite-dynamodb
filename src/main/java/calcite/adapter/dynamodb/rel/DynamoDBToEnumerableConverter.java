package calcite.adapter.dynamodb.rel;

import calcite.adapter.dynamodb.DynamoDBTable;
import com.google.common.collect.Lists;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.*;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;

import java.util.List;

/**
 * Relational expression representing a scan of a table in a DynamoDB data source.
 */
public class DynamoDBToEnumerableConverter
        extends ConverterImpl
        implements EnumerableRel {
    public DynamoDBToEnumerableConverter(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input) {
        super(cluster, ConventionTraitDef.INSTANCE, traits, input);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new DynamoDBToEnumerableConverter(
                getCluster(), traitSet, sole(inputs));
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy(.1);
    }

    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        final DynamoDBRel.Implementor dynamoDBImplementor = new DynamoDBRel.Implementor();
        dynamoDBImplementor.visitChild(0, getInput());

        PhysType physType =
                PhysTypeImpl.of(
                        implementor.getTypeFactory(),
                        getRowType(),
                        pref.preferArray());


        final BlockBuilder blockBuilder = new BlockBuilder();
        final Expression filterExpressions =
                blockBuilder.append("filterExpressions",
                        constantArrayList(dynamoDBImplementor.getFilterExpressions(), String.class));
        final Expression expressionAttributeNames =
                blockBuilder.append("expressionAttributeNames",
                        constantArrayList(dynamoDBImplementor.getExpressionAttributeNames(), String.class));
        final Expression expressionAttributeValues =
                blockBuilder.append("expressionAttributeValues",
                        constantArrayList(dynamoDBImplementor.getExpressionAttributeValues(), Object.class));
        final Expression keyConditionExpressions =
                blockBuilder.append("keyConditionExpressions",
                        constantArrayList(dynamoDBImplementor.getKeyConditionExpressions(), String.class));

        blockBuilder.add(Expressions.call(dynamoDBImplementor.table.getExpression(DynamoDBTable.class),
                "scanOrQuery", implementor.getRootExpression(),
                Expressions.constant(dynamoDBImplementor.getProjectionExpression()),
                filterExpressions,
                expressionAttributeNames,
                expressionAttributeValues,
                keyConditionExpressions
        ));

        return implementor.result(
                physType,
                Blocks.toBlock(blockBuilder.toBlock()));
    }

    /**
     * E.g. {@code constantArrayList("x", "y")} returns
     * "Arrays.asList('x', 'y')".
     */
    private static <T> MethodCallExpression constantArrayList(List<T> values,
                                                              Class clazz) {
        return Expressions.call(
                BuiltInMethod.ARRAYS_AS_LIST.method,
                Expressions.newArrayInit(clazz, constantList(values)));
    }

    /**
     * E.g. {@code constantList("x", "y")} returns
     * {@code {ConstantExpression("x"), ConstantExpression("y")}}.
     */
    private static <T> List<Expression> constantList(List<T> values) {
        return Lists.transform(values,
                a0 -> Expressions.constant(a0));
    }
}
