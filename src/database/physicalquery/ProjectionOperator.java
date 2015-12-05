package database.physicalquery;

import java.util.List;
import database.DbManager;
import storageManager.Block;
import storageManager.Relation;
import storageManager.Tuple;

public class ProjectionOperator extends OperatorBase
		implements OperatorInterface {
	public List<String> selectList;
	final int BLOCK_FOR_READING = 9;

	public ProjectionOperator(DbManager manager, List<String> selectList) {
		super(manager);
		this.selectList = selectList;
	}

	@Override
	public List<Tuple> execute(boolean printResult) {
		System.out.println("******FINAL RESULT*********");
		for (String col : selectList)
			System.out.print(col + "\t");
		System.out.println("");

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
			if (printResult) {
				for (String col : selectList) {
					System.out.print(tuple.getField(col).toString() + "\t");
				}
				System.out.println("");
			}
			res_tuples.add(tuple);
		}
	}

}