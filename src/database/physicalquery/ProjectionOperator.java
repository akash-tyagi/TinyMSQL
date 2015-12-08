package database.physicalquery;

import java.io.PrintWriter;
import java.util.List;

import database.DbManager;
import database.GlobalVariable;
import database.utils.GeneralUtils;
import storageManager.Block;
import storageManager.Relation;
import storageManager.Tuple;

public class ProjectionOperator extends OperatorBase
		implements OperatorInterface {
	public List<String> selectList;
	final int BLOCK_FOR_READING = 9;

	public ProjectionOperator(DbManager manager, List<String> selectList,
			PrintWriter writer) {
		super(manager, writer);
		this.selectList = selectList;
	}

	@Override
	public List<Tuple> execute(boolean printResult) {
		if (GlobalVariable.isTestExecution) {
			for (String col : selectList)
				System.out.print(col + "\t");
			System.out.println("");
		}
		if (isReadFromMem) {
			System.out.println("PROJECT FROM MEM");
			readFromMemory(printResult);
		} else {
			System.out.println("FROM DISK");
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
			for (String col : selectList) {
				col = getFieldName(tuple, col);
				System.out.print(tuple.getField(col).toString() + "\t");
				writer.print(tuple.getField(col).toString() + "\t");
			}
			System.out.println("");
			writer.println();
		}
		res_tuples.add(tuple);
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