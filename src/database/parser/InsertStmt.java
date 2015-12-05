package database.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import database.DbManager;
import database.GlobalVariable;
import database.physicalquery.PhysicalTree;
import database.physicalquery.SelectOperator;
import database.utils.GeneralUtils;
import storageManager.Block;
import storageManager.Field;
import storageManager.FieldType;
import storageManager.MainMemory;
import storageManager.Relation;
import storageManager.Schema;
import storageManager.Tuple;

public class InsertStmt extends StmtBase implements StmtInterface {

	String tableName;
	List<String> attrList;
	List<String> valueList;
	StmtInterface selectStmt;
	List<Tuple> res_tuples;

	public InsertStmt(DbManager dbManager) {
		super(dbManager);
		attrList = new ArrayList<>();
		valueList = new ArrayList<>();
	}

	private void parseAttibuteList(String attrList2) {
		Pattern pattern = Pattern.compile("([a-z][a-z0-9]*)\\s*(,*\\s*.*)");
		Matcher matcher = pattern.matcher(attrList2);
		if (GlobalVariable.isTestParsing)
			System.out.println("INSERT Statement: PARSING Attribute List");
		while (matcher.find()) {
			String attrName = matcher.group(1);
			attrName = attrName.trim();
			attrList.add(attrName);
			if (GlobalVariable.isTestParsing)
				System.out.println("INSERT Statement: AttrName:" + attrName);
			attrList2 = matcher.group(2);
			matcher = pattern.matcher(attrList2);
		}
	}

	private void tuplesToValueList() {
		for (Tuple tuple : res_tuples) {
			for (int i = 0; i < tuple.getNumOfFields(); i++) {
				valueList.add(tuple.getField(i).toString());
			}
			execute();
			valueList = new ArrayList<>();
		}
	}

	private void parseTuples(String tuples) {
		if (tuples.contains("SELECT")) {
			System.out.println(tuples);
			Parser parser = new Parser(dbManager);
			StmtInterface stmt = parser.parse(tuples);
			PhysicalTree physicalTree = new PhysicalTree(dbManager, stmt, null);
			SelectOperator selectOperator = (SelectOperator) physicalTree.operator;
			res_tuples = selectOperator.execute(true);
			return;
		}
		if (GlobalVariable.isTestParsing)
			System.out.println("INSERT Statement: PARSING Value List");
		String[] list = tuples.split(",");
		for (String value : list) {
			value = value.trim();
			// value = value.replaceAll("\"", "");
			valueList.add(value);
			if (GlobalVariable.isTestParsing)
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
			if (GlobalVariable.isTestParsing)
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
			if (GlobalVariable.isTestParsing)
				System.out.println("INSERT Statement: AttrList:" + attrList
						+ " Tuples:" + tuples);
			parseAttibuteList(attrList);
			parseTuples(tuples);
		} else {
			System.out.println("ERROR ::: INSERT statement: Invalid:" + query);
			System.exit(1);
		}
		if (res_tuples == null)
			execute();
		else
			tuplesToValueList();
	}

	@Override
	public void execute() {
		// NOTE : TYPE CHECKING IS REQUIRED
		Relation relation_reference = dbManager.schema_manager
				.getRelation(tableName);
		Schema schema = relation_reference.getSchema();
		ArrayList<FieldType> field_types = schema.getFieldTypes();
		ArrayList<String> field_names = schema.getFieldNames();
		// POINT 1 : EVEN THOUGH RELATION RETURNS TUPLE. THIS IS JUST A TUPLE
		// "new Tuple()"
		// WITH NO LINK WHATSOEVER WITH THE RELATION WHICH CREATES THIS TUPLE
		// EXCEPT FOR THE FACT THAT IT GETS THE SCHEMA INFORMATION FOR THE
		// RELATION.
		Tuple tuple = relation_reference.createTuple();
		if (attrList.size() != valueList.size()) {
			for (int i = 0; i < valueList.size(); i++) {
				// No need to set Field if value is "NULL"
				// Field.str defaults to null
				// Field.integer defaults to Integer.MIN_VALUE
				if (valueList.get(i).equalsIgnoreCase("NULL")) {
					updateVTable(i, field_names);
					continue;
				}

				if (field_types.get(i) == FieldType.STR20) {
					tuple.setField(i, valueList.get(i));
				} else {
					tuple.setField(i, Integer.parseInt(valueList.get(i)));
				}

				// Update vTable
				updateVTable(i, field_names);
			}
		} else {
			for (int i = 0; i < valueList.size(); i++) {
				// No need to set Field if value is "NULL"
				// Field.str defaults to null
				// Field.integer defaults to Integer.MIN_VALUE
				if (valueList.get(i).equalsIgnoreCase("NULL")) {
					updateVTable(i);
					continue;
				}

				int myIndex = field_names.indexOf(attrList.get(i));
				if (field_types.get(myIndex) == FieldType.STR20) {
					tuple.setField(myIndex, valueList.get(i));
				} else {
					tuple.setField(myIndex, Integer.parseInt(valueList.get(i)));
				}
				// Update vTable
				updateVTable(i);
			}
		}
		GeneralUtils.appendTupleToRelation(relation_reference, dbManager.mem, 0,
				tuple);
		// Block block_reference = dbManager.mem.getBlock(0);
		// block_reference.appendTuple(tuple);
		// // NOTE : BELOW STEP IS NECESSARY. SEE POINT 1 ABOVE
		// relation_reference.setBlock(relation_reference.getNumOfBlocks(), 0);
	}

	// updateVTable keeps NULL values as "NULL" String
	private void updateVTable(int fieldPtr, ArrayList<String> field_names) {
		Integer curValue = dbManager.vTable.get(tableName)
				.get(field_names.get(fieldPtr)).get(valueList.get(fieldPtr));
		if (curValue != null) {
			dbManager.vTable.get(tableName).get(field_names.get(fieldPtr))
					.put(valueList.get(fieldPtr), curValue + 1);
		} else {
			dbManager.vTable.get(tableName).get(field_names.get(fieldPtr))
					.put(valueList.get(fieldPtr), 1);
		}
	}

	private void updateVTable(int fieldPtr) {
		Integer curValue = dbManager.vTable.get(tableName)
				.get(attrList.get(fieldPtr)).get(valueList.get(fieldPtr));
		if (curValue != null) {
			dbManager.vTable.get(tableName).get(attrList.get(fieldPtr))
					.put(valueList.get(fieldPtr), curValue + 1);
		} else {
			dbManager.vTable.get(tableName).get(attrList.get(fieldPtr))
					.put(valueList.get(fieldPtr), 1);
		}
	}

}
