package calcite.adapter.dynamodb.rules;

import calcite.adapter.dynamodb.rel.DynamoDBFilter;
import calcite.adapter.dynamodb.rel.DynamoDBRel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalFilter;

public class DynamoDBFilterRule extends ConverterRule {
    private String hashKeyName;
    private String sortKeyName;

    private static DynamoDBFilterRule INSTANCE = null;

    public static DynamoDBFilterRule getInstance(String hashKeyName, String sortKeyName) {
        if (INSTANCE == null) {
            INSTANCE = new DynamoDBFilterRule(hashKeyName, sortKeyName);
        }
        return INSTANCE;
    }

    private DynamoDBFilterRule(String hashKeyName, String sortKeyName) {
        super(LogicalFilter.class, Convention.NONE, DynamoDBRel.CONVENTION, "DynamoDBFilterRule");

        this.hashKeyName = hashKeyName;
        this.sortKeyName = sortKeyName;
    }

    @Override
    public RelNode convert(RelNode rel) {
        final LogicalFilter filter = (LogicalFilter) rel;
        final RelTraitSet traitSet = filter.getTraitSet().replace(DynamoDBRel.CONVENTION);
        return new DynamoDBFilter(
                rel.getCluster(),
                traitSet,
                convert(filter.getInput(), DynamoDBRel.CONVENTION),
                filter.getCondition(), hashKeyName, sortKeyName);
    }
}