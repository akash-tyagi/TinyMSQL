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
	public void execute() {
		if (isReadFromMem) {
			sortInMemory();
			if (next_operator != null)
				next_operator.setBlocksNumbers(start_block, end_block);
		} else {
//			sortDisk();  // return the sorted table name
			if (next_operator != null)
				next_operator.setRelationName(relation_name);
		}
		if (next_operator != null)
			next_operator.execute();
	}

	private void sortDisk() {
	}

	private void sortInMemory() {
		List<String> colList = new ArrayList<String>();
		colList.add(column_name);
		GeneralUtils.sortMainMemory(dbManager, colList, end_block + 1);
		if (next_operator == null)
			for (int i = start_block; i <= end_block; i++)
				printMemBlock(dbManager.mem.getBlock(i));
	}

	private void printMemBlock(Block block) {
		List<Tuple> tuples = block.getTuples();
		for (Tuple tuple : tuples) {
			System.out.println(tuple.toString());
		}
	}

}
