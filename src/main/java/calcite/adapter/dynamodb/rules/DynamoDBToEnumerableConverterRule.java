package calcite.adapter.dynamodb.rules;

import calcite.adapter.dynamodb.rel.DynamoDBRel;
import calcite.adapter.dynamodb.rel.DynamoDBToEnumerableConverter;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

public class DynamoDBToEnumerableConverterRule extends ConverterRule {
    public static final ConverterRule INSTANCE =
            new DynamoDBToEnumerableConverterRule();

    private DynamoDBToEnumerableConverterRule() {
        super(RelNode.class, DynamoDBRel.CONVENTION, EnumerableConvention.INSTANCE,
                "DynamoDBToEnumerableConverterRule");
    }

    @Override
    public RelNode convert(RelNode rel) {
        RelTraitSet newTraitSet = rel.getTraitSet().replace(getOutConvention());

        return new DynamoDBToEnumerableConverter(rel.getCluster(), newTraitSet, rel);
    }
}
