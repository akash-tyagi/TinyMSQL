package database.physicalquery;

import java.util.ArrayList;
import java.util.List;

import database.DbManager;
import database.utils.GeneralUtils;
import storageManager.Block;
import storageManager.Tuple;

public class SortingOperator extends OperatorBase implements OperatorInterface {
	String column_name;

	public SortingOperator(DbManager dbManager, String column_name) {
		super(dbManager);
		this.column_name = column_name;
	}

	@Override
	public List<Tuple> execute(boolean printResult) {
		if (isReadFromMem) {
			sortInMemory(printResult);
			if (next_operator != null)
				next_operator.setBlocksNumbers(start_block, end_block);
		} else {
			// sortDisk(printResult); // return the sorted table name
			if (next_operator != null)
				next_operator.setRelationName(relation_name);
		}
		if (next_operator != null)
			return next_operator.execute(printResult);
		return res_tuples;
	}

	private void sortDisk(boolean printResult) {
	}

	private void sortInMemory(boolean printResult) {
		List<String> colList = new ArrayList<String>();
		colList.add(column_name);
		GeneralUtils.sortMainMemory(dbManager, colList, end_block + 1);
		if (next_operator == null)
			for (int i = start_block; i <= end_block; i++)
				printMemBlock(dbManager.mem.getBlock(i), printResult);
	}

	private void printMemBlock(Block block, boolean printResult) {
		List<Tuple> tuples = block.getTuples();
		for (Tuple tuple : tuples) {
			if (printResult)
				System.out.println(tuple.toString());
			res_tuples.add(tuple);
		}
	}

}
