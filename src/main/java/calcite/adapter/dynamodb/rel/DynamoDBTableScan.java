package calcite.adapter.dynamodb.rel;

import calcite.adapter.dynamodb.DynamoDBTable;
import calcite.adapter.dynamodb.rules.DynamoDBFilterRule;
import calcite.adapter.dynamodb.rules.DynamoDBProjectRule;
import calcite.adapter.dynamodb.rules.DynamoDBToEnumerableConverterRule;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

public class DynamoDBTableScan extends TableScan implements DynamoDBRel {
    final DynamoDBTable dynamoDBTable;
    final RelDataType projectRowType;

    private String hashKeyName;
    private String sortKeyName;

    public DynamoDBTableScan(RelOptCluster cluster, RelTraitSet traitSet,
                             RelOptTable table, DynamoDBTable dynamoDBTable, RelDataType projectRowType,
                             String hashKeyName, String sortKeyName) {
        super(cluster, traitSet, table);
        this.dynamoDBTable = dynamoDBTable;
        this.projectRowType = projectRowType;

        this.hashKeyName = hashKeyName;
        this.sortKeyName = sortKeyName;

        assert dynamoDBTable != null;
        assert getConvention() == DynamoDBRel.CONVENTION;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.isEmpty();
        return this;
    }

    @Override
    public RelDataType deriveRowType() {
        return projectRowType != null ? projectRowType : super.deriveRowType();
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
        // scans with a small project list are cheaper
        final float f = projectRowType == null ? 1f
                : (float) projectRowType.getFieldCount() / 100f;
        return super.computeSelfCost(planner, mq).multiplyBy(.1 * f);
    }

    @Override
    public void register(RelOptPlanner planner) {
        planner.addRule(DynamoDBToEnumerableConverterRule.INSTANCE);
        planner.addRule(DynamoDBProjectRule.INSTANCE);
        planner.addRule(DynamoDBFilterRule.getInstance(hashKeyName, sortKeyName));
    }

    public void implement(Implementor implementor) {
        implementor.table = table;
    }
}
