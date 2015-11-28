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
	public void execute() {
		System.out.println("******FINAL RESULT*********");
		for (String col : selectList)
			System.out.print(col + "\t");
		System.out.println("");

		if (isReadFromMem) {
			readFromMemory();
		} else {
			readFromDisk();
		}
	}

	private void readFromDisk() {
		Relation rel = dbManager.schema_manager.getRelation(relation_name);
		for (int i = 0; i < rel.getNumOfBlocks(); i++) {
			rel.getBlock(i, BLOCK_FOR_READING);
			printMemBlock(dbManager.mem.getBlock(BLOCK_FOR_READING));
		}
	}

	private void readFromMemory() {
		for (int i = START_BLOCK; i <= FINAL_BLOCK; i++)
			printMemBlock(dbManager.mem.getBlock(i));
	}

	private void printMemBlock(Block block) {
		List<Tuple> tuples = block.getTuples();
		for (Tuple tuple : tuples) {
			for (String col : selectList) {
				System.out.print(tuple.getField(col).toString() + "\t");
			}
			System.out.println("");
		}
	}

}