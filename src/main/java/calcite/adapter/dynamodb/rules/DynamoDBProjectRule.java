package calcite.adapter.dynamodb.rules;

import calcite.adapter.dynamodb.rel.DynamoDBProject;
import calcite.adapter.dynamodb.rel.DynamoDBRel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalProject;

public class DynamoDBProjectRule extends ConverterRule {
    public static final DynamoDBProjectRule INSTANCE =
            new DynamoDBProjectRule();

    private DynamoDBProjectRule() {
        super(LogicalProject.class, Convention.NONE, DynamoDBRel.CONVENTION, "DynamoDBProjectRule");
    }

    @Override
    public RelNode convert(RelNode rel) {
        final LogicalProject project = (LogicalProject) rel;
        final RelTraitSet traitSet = project.getTraitSet().replace(DynamoDBRel.CONVENTION);
        return new DynamoDBProject(project.getCluster(), traitSet,
                convert(project.getInput(), DynamoDBRel.CONVENTION), project.getProjects(),
                project.getRowType());
    }
}