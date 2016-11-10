# Calcite DynamoDB Adapter

Calcite DynamoDB Adapter is SQL interface for [Amazon DynamoDB](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Introduction.html).

----
## How to compile the source code and run unit tests
Make sure you installed Maven and Java8, in the project home directory run maven command:

    mvn clean package

----
## How it works
Calcite DynamoDB Adapter is build on top of [Apache Calcite](https://calcite.apache.org/), when connect it via JDBC, the connect string contains a json model file:
```json
{
  "version": "1.0",
  "defaultSchema": "DEMO",
  "schemas": [
    {
      "name": "DEMO",
      "type": "custom",
      "factory": "calcite.adapter.dynamodb.DynamoDBSchemaFactory",
      "operand": {
        "region": "us-west-2",
        "meta": "meta"
      }
    }
  ]
}
```
In this json model file, `"region": "us-west-2"` means DynamoDB region is us-west-2, and `"meta": "meta"` means the DynamoDB table "meta" stores table metadata.

Then Calcite DynamoDB Adapter will scan DynamoDB table "meta" to generate all of the available tables under `DEMO` schema.

More details please see [unit test](src/test/java/calcite/adapter/dynamodb/TestDynamoDBAdapter.java)
