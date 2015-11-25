package database.parser;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import storageManager.FieldType;
import storageManager.Relation;
import storageManager.Schema;
import database.DbManager;
import java.util.HashMap;

public class CreateStmt extends StmtBase implements StmtInterface {

    String relation_name;
    ArrayList<String> field_names;
    ArrayList<FieldType> field_types;

    public CreateStmt(DbManager dbManager) {
        super(dbManager);
        field_names = new ArrayList<>();
        field_types = new ArrayList<>();
    }

    private void parseAttibuteList(String attrList) {
        Pattern pattern = Pattern
                .compile("([a-z][a-z0-9]*)\\s*(INT|STR20)\\s*(,*\\s*.*)");
        Matcher matcher = pattern.matcher(attrList);

        while (matcher.find()) {
            String fieldName = matcher.group(1);
            String fieldType = matcher.group(2);
            if (fieldName.isEmpty() || fieldType.isEmpty()) {
                System.out
                        .println("ERROR ::: CREATE statement: Invalid Attributes:"
                                + attrList);
                System.exit(1);
            }
            field_names.add(fieldName);
            field_types.add(fieldType.contains("STR") ? FieldType.STR20
                    : FieldType.INT);
            System.out.println("CREATE Statement: AttrName:" + fieldName
                    + "      AttrType:" + fieldType);
            attrList = matcher.group(3);
            matcher = pattern.matcher(attrList);
        }
    }

    public void create(String query) {
        Pattern pattern = Pattern
                .compile("CREATE TABLE ([a-z][a-z0-9]*)\\s*[(]\\s*(.*)\\s*[)]");
        Matcher matcher = pattern.matcher(query);
        if (matcher.find()) {
            relation_name = matcher.group(1);
            String attrList = matcher.group(2);
            System.out.println("CREATE Statement: TableName:" + relation_name);
            System.out.println("CREATE Statement: AttrList:" + attrList);
            parseAttibuteList(attrList);
        } else {
            System.out.println("ERROR ::: CREATE statement: Invalid:" + query);
            System.exit(1);
        }
        execute();
    }

    public void execute1() {
//		System.out
//				.print("=======================Schema========================="
//						+ "\n");
//		// Create a schema
//		System.out.print("Creating a schema" + "\n");
//		Schema schema = new Schema(field_names, field_types);
//
//		// =========Relation & SchemaManager=========
//		System.out.print("=========Relation & SchemaManager======" + "\n");
//		System.out.print("Creating table " + relation_name + "\n");
//		Relation relation_reference = manager.schema_manager.createRelation(
//				relation_name, schema);
//
//		// Print the information about the Relation
//		System.out.print("The table has name "
//				+ relation_reference.getRelationName() + "\n");
//		System.out.print("The table has schema:" + "\n");
//		System.out.print(relation_reference.getSchema() + "\n");
//		System.out.print("The table currently have "
//				+ relation_reference.getNumOfBlocks() + " blocks" + "\n");
//		System.out
//				.print("The table currently have "
//						+ relation_reference.getNumOfTuples() + " tuples"
//						+ "\n" + "\n");
//
//		// Print the information provided by the schema manager
//		System.out.print("Current schemas and relations: " + "\n");
//		System.out.print(manager.schema_manager + "\n");
//		System.out
//				.print("From the schema manager, the table "
//						+ relation_name
//						+ " exists: "
//						+ (manager.schema_manager.relationExists(relation_name) ? "TRUE"
//								: "FALSE") + "\n");
//		System.out.print("From the schema manager, the table " + relation_name
//				+ " has schema:" + "\n");
//		System.out
//				.print(manager.schema_manager.getSchema(relation_name) + "\n");
//		System.out.print("From the schema manager, the table " + relation_name
//				+ " has schema:" + "\n");
//		System.out.print(manager.schema_manager.getRelation(relation_name)
//				.getSchema() + "\n");

    }

    @Override
    public void execute() {
        Schema schema = new Schema(field_names, field_types);
        Relation relation_reference = dbManager.schema_manager.createRelation(relation_name, schema);

        // Make a new entry to vTable for this relation
        HashMap<String, HashMap<String, Integer>> columnMap = new HashMap<>();
        for (String field_name : field_names) {
            columnMap.put(field_name, new HashMap<>());
        }
        dbManager.vTable.put(relation_name, columnMap);

    }
}
