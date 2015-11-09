package database.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import database.DbManager;
import storageManager.Block;
import storageManager.FieldType;
import storageManager.Relation;
import storageManager.Schema;
import storageManager.Tuple;

public class InsertStmt extends StmtBase implements StmtInterface {

	String tableName;
	List<String> attrList;
	List<String> valueList;
	StmtInterface selectStmt;

	public InsertStmt(DbManager dbManager) {
		super(dbManager);
		attrList = new ArrayList<>();
		valueList = new ArrayList<>();
	}

	private void parseAttibuteList(String attrList2) {
		Pattern pattern = Pattern.compile("([a-z][a-z0-9]*)\\s*(,*\\s*.*)");
		Matcher matcher = pattern.matcher(attrList2);
		System.out.println("INSERT Statement: PARSING Attribute List");
		while (matcher.find()) {
			String attrName = matcher.group(1);
			attrName = attrName.trim();
			attrList.add(attrName);
			System.out.println("INSERT Statement: AttrName:" + attrName);
			attrList2 = matcher.group(2);
			matcher = pattern.matcher(attrList2);
		}
	}

	private void parseTuples(String tuples) {
		if (tuples.contains("SELECT")) {
			selectStmt = new SelectStmt(dbManager);
			selectStmt.create(tuples);
			return;
		}
		System.out.println("INSERT Statement: PARSING Value List");
		String[] list = tuples.split(",");
		for (String value : list) {
			value = value.trim();
			// value = value.replaceAll("\"", "");
			valueList.add(value);
			System.out.println("INSERT Statement: value:" + value);
		}
	}

	@Override
	public void create(String query) {
		String attrList = "", tuples = "";
		Pattern pattern = Pattern.compile("INSERT INTO ([a-z][a-z0-9]*)(.*)");
		Matcher matcher = pattern.matcher(query);
		if (matcher.find()) {
			tableName = matcher.group(1);
			System.out.println("INSERT Statement: TableName:" + tableName);
			String rawList = matcher.group(2);
			if (rawList.contains("VALUES")) {
				pattern = Pattern
						.compile("\\s*[(](.*)[)]\\s*VALUES\\s*[(](.*)[)]");
				matcher = pattern.matcher(rawList);
				if (matcher.find()) {
					attrList = matcher.group(1);
					tuples = matcher.group(2);
				}
			} else {
				pattern = Pattern.compile("\\s*[(](.*)[)]\\s*(SELECT .*)");
				matcher = pattern.matcher(rawList);
				if (matcher.find()) {
					attrList = matcher.group(1);
					tuples = matcher.group(2);
				}
			}
			System.out.println("INSERT Statement: AttrList:" + attrList
					+ " Tuples:" + tuples);
			parseAttibuteList(attrList);
			parseTuples(tuples);
		} else {
			System.out.println("ERROR ::: INSERT statement: Invalid:" + query);
			System.exit(1);
		}

		execute();
	}

	@Override
	public void execute() {
		// NOTE : TYPE CHECKING IS REQUIRED
		Relation relation_reference = dbManager.schema_manager
				.getRelation(tableName);
		Schema schema = relation_reference.getSchema();
		ArrayList<FieldType> field_types = schema.getFieldTypes();
		// POINT 1 : EVEN THOUGH RELATION RETURNS TUPLE. THIS IS JUST A TUPLE
		// "new Tuple()"
		// WITH NO LINK WHATSOEVER WITH THE RELATION WHICH CREATES THIS TUPLE.
		// ASSUME FOR NOW THAT THIS IS JUST A DESIGN FLAW.
		Tuple tuple = relation_reference.createTuple();
		if (attrList.size() != valueList.size()) {
			for (int i = 0; i < valueList.size(); i++) {
				if (field_types.get(i) == FieldType.STR20) {
					tuple.setField(i, valueList.get(i));
				} else {
					tuple.setField(i, Integer.parseInt(valueList.get(i)));
				}
			}
		} else {
			for (int i = 0; i < valueList.size(); i++) {
				ArrayList<String> field_names = schema.getFieldNames();
				int myIndex = field_names.indexOf(attrList.get(i));
				if (field_types.get(myIndex) == FieldType.STR20) {
					tuple.setField(myIndex, valueList.get(i));
				} else {
					tuple.setField(myIndex, Integer.parseInt(valueList.get(i)));
				}
			}
		}
		Block block_reference = dbManager.mem.getBlock(0);
		block_reference.appendTuple(tuple);
		System.out.println(relation_reference.getNumOfTuples());
		// NOTE : BELOW STEP IS NECESSARY. SEE POINT 1 ABOVE
		relation_reference.setBlock(relation_reference.getNumOfBlocks(), 0);
	}
}
