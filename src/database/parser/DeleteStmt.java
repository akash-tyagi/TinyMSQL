package database.parser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import database.DbManager;
import database.GlobalVariable;
import database.parser.searchcond.SearchCond;
import java.util.ArrayList;
import storageManager.Block;
import storageManager.FieldType;
import storageManager.Relation;
import storageManager.Schema;
import storageManager.Tuple;

public class DeleteStmt extends StmtBase implements StmtInterface {

	String tableName;
	SearchCond cond;

	public DeleteStmt(DbManager manager) {
		super(manager);
	}

	@Override
	public void create(String query) {
		Pattern pattern = Pattern.compile("DELETE FROM (.*)");
		int type = 0;
		if (query.contains("WHERE")) {
			pattern = Pattern
					.compile("DELETE FROM\\s*([a-z][a-z0-9]*)\\s* WHERE (.*)");
			type = 1;
		}
		Matcher matcher = pattern.matcher(query);
		if (matcher.find()) {
			tableName = matcher.group(1);
			if (GlobalVariable.isTestParsing)
				System.out.println("DELETE Statement: TableName:" + tableName);
			if (type == 1) {
				cond = new SearchCond();
				cond.create(matcher.group(2));
			}
		} else {
			System.out.println("ERROR ::: DELETE statement: Invalid:" + query);
			System.exit(1);
		}
		execute();
	}

	@Override
	public void execute() {
		Block last_block = dbManager.mem
				.getBlock(dbManager.mem.getMemorySize() - 1);
		last_block.clear();
		int diskPointer = 0;
		Relation relation = dbManager.schema_manager.getRelation(tableName);
		int memIndex = 0;
		for (int i = 0; i < relation.getNumOfBlocks(); i++) {
			relation.getBlock(i, memIndex);
			Block block_reference = dbManager.mem.getBlock(memIndex);
			for (int j = 0; j < block_reference.getNumTuples(); j++) {
				Tuple tuple = block_reference.getTuple(j);
				// Assumption: deleting all tuples if where cond is null
				if (!(cond == null || cond.execute(tuple))) {
					boolean blockWrittenToDisk = saveTupleToLastBlock(tuple,
							last_block, relation, diskPointer,
							dbManager.mem.getMemorySize() - 1);
					if (blockWrittenToDisk) {
						diskPointer++;
					}
				} else {
					// Update vTable
					Schema schema = relation.getSchema();
					ArrayList<String> field_names = schema.getFieldNames();

					for (String field_name : field_names) {
						String field_value = tuple.getField(field_name)
								.toString();

						// If the field value was stored null
						// Default value (when "NULL" is passed) for integer is
						// Integer.MIN_VALUE
						// and for str is null
						FieldType field_type = schema.getFieldType(field_name);
						if (field_type == FieldType.INT) {
							if (Integer.parseInt(
									field_value) == Integer.MIN_VALUE) {
								field_value = "NULL";
							}
						} else {
							if (field_value.equals("null")) {
								field_value = "NULL";
							}
						}

						updateVTable(field_name, field_value);
					}
				}

			}

		}

		if (!last_block.isEmpty()) {
			relation.setBlock(diskPointer, dbManager.mem.getMemorySize() - 1);
			diskPointer++;
		}

		relation.deleteBlocks(diskPointer);

	}

	private boolean saveTupleToLastBlock(Tuple tuple, Block last_block,
			Relation relation, int diskPointer, int temp_location) {
		last_block.appendTuple(tuple);
		if (last_block.isFull()) {
			relation.setBlock(diskPointer, temp_location);
			last_block.clear();
			return true;
		}
		return false;
	}

	private void updateVTable(String field_name, String field_value) {
		int curValue = dbManager.vTable.get(tableName).get(field_name)
				.get(field_value);
		curValue--;
		if (curValue == 0) {
			dbManager.vTable.get(tableName).get(field_name).remove(field_value);
		} else {
			dbManager.vTable.get(tableName).get(field_name).put(field_value,
					curValue);
		}
	}

}
