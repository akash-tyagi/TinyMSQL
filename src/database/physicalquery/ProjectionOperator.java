package database.physicalquery;

import java.io.PrintWriter;
import java.util.List;

import database.DbManager;
import database.GlobalVariable;
import storageManager.Block;
import storageManager.Relation;
import storageManager.Tuple;

public class ProjectionOperator extends OperatorBase
		implements OperatorInterface {
	public List<String> selectList;
	final int BLOCK_FOR_READING = 9;
	boolean isJoin;
	boolean printHeader;
	List<String> joinColumns;

	public ProjectionOperator(DbManager manager, List<String> selectList,
			PrintWriter writer, boolean isJoin, List<String> list) {
		super(manager, writer);
		this.selectList = selectList;
		this.isJoin = isJoin;
		this.joinColumns = list;
		printHeader = true;
	}

	@Override
	public List<Tuple> execute(boolean printResult) {
		if (GlobalVariable.isTestExecution) {
			for (String col : selectList)
				System.out.print(col + "\t");
			System.out.println("");
		}
		if (isReadFromMem) {
			readFromMemory(printResult);
		} else {
			readFromDisk(printResult);
		}
		return res_tuples;
	}

	private void readFromDisk(boolean printResult) {
		Relation rel = dbManager.schema_manager.getRelation(relation_name);
		for (int i = 0; i < rel.getNumOfBlocks(); i++) {
			rel.getBlock(i, BLOCK_FOR_READING);
			printMemBlock(dbManager.mem.getBlock(BLOCK_FOR_READING),
					printResult);
		}
	}

	private void readFromMemory(boolean printResult) {
		for (int i = start_block; i <= end_block; i++)
			printMemBlock(dbManager.mem.getBlock(i), printResult);
	}

	private void printMemBlock(Block block, boolean printResult) {
		List<Tuple> tuples = block.getTuples();
		for (Tuple tuple : tuples) {
			printTuple(tuple, printResult);
		}
	}

	public void printTuple(Tuple tuple, boolean printResult) {
		if (printResult) {
			if (printHeader) {
				printHeader(tuple, printResult);
				printHeader = false;
			}
			if (selectList != null)
				for (String col : selectList) {
					col = getFieldName(tuple, col);
					System.out.print(tuple.getField(col).toString() + "\t");
					writer.print(tuple.getField(col).toString() + "\t");
				}
			else {
				for (String field_name : tuple.getSchema().getFieldNames()) {
					System.out.print(
							tuple.getField(field_name).toString() + "\t");
					writer.print(tuple.getField(field_name).toString() + "\t");
					if (isJoin) {
						for (String joinCol : joinColumns) {
							if (field_name.endsWith("." + joinCol)) {
								System.out.print(
										tuple.getField(field_name).toString()
												+ "\t");
								writer.print(
										tuple.getField(field_name).toString()
												+ "\t");
								break;
							}
						}
					}
				}
			}
			System.out.println("");
			writer.println();
		}
		res_tuples.add(tuple);
	}

	private void printHeader(Tuple tuple, boolean printResult) {
		if (selectList != null)
			for (String col : selectList)
				printColName(col);
		else
			for (String field_name : tuple.getSchema().getFieldNames())
				printColName(field_name);
		System.out.println("");
		writer.println();
	}

	private void printColName(String col) {
		String[] list = col.split("\\.");
		col = list[list.length - 1];
		System.out.print(col + "\t");
		writer.print(col + "\t");
		if (isJoin) {
			for (String joinCol : joinColumns) {
				if (col.equals(joinCol)) {
					System.out.print(col + "\t");
					writer.print(col + "\t");
					break;
				}
			}
		}
	}

	private String getFieldName(Tuple t, String col) {
		List<String> field_names = t.getSchema().getFieldNames();
		if (field_names.contains(col)) {
			return col;
		} else {
			String[] strs = col.split("\\.");
			for (String field : field_names) {
				if (field.endsWith("." + strs[1])
						&& (field.contains(strs[0] + ".")
								|| field.contains(strs[0] + "_"))) {
					return field;
				}
			}
		}
		return null;
	}
}