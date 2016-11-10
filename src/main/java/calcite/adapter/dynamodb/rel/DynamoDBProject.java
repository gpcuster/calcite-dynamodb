package calcite.adapter.dynamodb.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;

import java.util.List;

/**
 * Implementation of {@link org.apache.calcite.rel.core.Project}
 * relational expression in DynamoDB.
 */
public class DynamoDBProject extends Project implements DynamoDBRel {
    public DynamoDBProject(RelOptCluster cluster, RelTraitSet traitSet,
                           RelNode input, List<? extends RexNode> projects, RelDataType rowType) {
        super(cluster, traitSet, input, projects, rowType);
        assert getConvention() == DynamoDBRel.CONVENTION;
        assert getConvention() == input.getConvention();
    }

    @Override
    public Project copy(RelTraitSet traitSet, RelNode input,
                        List<RexNode> projects, RelDataType rowType) {
        return new DynamoDBProject(getCluster(), traitSet, input, projects,
                rowType);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy(0.1);
    }

    public void implement(Implementor implementor) {
        implementor.visitChild(0, getInput());

        for (Pair<RexNode, String> namedProject : getNamedProjects()) {
            implementor.addAttributeToGet(namedProject.getValue());
        }
    }
}