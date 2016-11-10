package calcite.adapter.dynamodb;

import calcite.adapter.dynamodb.utils.DynamoDBClientUtil;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Map;

public class DynamoDBSchemaFactory implements SchemaFactory {
    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        String regionName = (String) operand.get("region");
        String metaTableName = (String) operand.get("meta");
        Boolean local = (Boolean) operand.get("local");
        if (local == null) {
            local = false;
        }

        return new DynamoDBSchema(DynamoDBClientUtil.createAmazonDynamoDBClient(regionName, local), metaTableName);
    }
}
