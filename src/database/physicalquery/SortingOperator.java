package database.physicalquery;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import database.DbManager;
import database.GlobalVariable;
import database.utils.GeneralUtils;
import database.utils.TupleObject;
import database.utils.TwoPassUtils;
import storageManager.Block;
import storageManager.Relation;
import storageManager.Tuple;

public class SortingOperator extends OperatorBase implements OperatorInterface {
	String column_name;

	public SortingOperator(DbManager dbManager, String column_name,
			PrintWriter writer) {
		super(dbManager, writer);
		this.column_name = column_name;
	}

	@Override
	public List<Tuple> execute(boolean printResult) {
		if (isReadFromMem) {
			sortInMemory(printResult);
			if (next_operator != null)
				next_operator.setBlocksNumbers(start_block, end_block);
		} else {
			int blocks = dbManager.schema_manager.getRelation(relation_name)
					.getNumOfBlocks();
			if (blocks <= GlobalVariable.TOTAL_DATA_BLOCKS) {
				System.out.println("CALLING ONE PASS SORTING");
				start_block = 0;
				end_block = blocks - 1;
				onePassSorting(printResult);
				if (next_operator != null)
					next_operator.setBlocksNumbers(start_block, end_block);

			} else {
				sortDisk(printResult);
				// if (next_operator != null)
				// next_operator.setRelationName(sorted_relation);
			}
		}
		if (next_operator != null)
			return next_operator.execute(printResult);
		return res_tuples;
	}

	private void onePassSorting(boolean printResult) {
		Relation rel = dbManager.schema_manager.getRelation(relation_name);
		int disk_index = 0;
		for (int i = start_block; i <= end_block; i++) {
			rel.getBlock(disk_index, i);
			disk_index++;
		}
		System.out.println("Start:" + start_block + " end:" + end_block);
		sortInMemory(printResult);
	}

	private void sortDisk(boolean printResult) {
		ArrayList<String> columns = new ArrayList<>();
		columns.add(column_name);
		twoPassSort(dbManager, columns, printResult);
	}

	private void twoPassSort(DbManager dbManager, ArrayList<String> sortingCols,
			boolean printResult) {
		if (dbManager.schema_manager.getRelation(relation_name)
				.getNumOfBlocks() > dbManager.mem.getMemorySize()
						* dbManager.mem.getMemorySize()) {
			System.out.println("ERROR: SORTING NOT POSSIBLE, RELATION TOO BIG");
			return;
		}
		TwoPassUtils.sortRelationForPhase1(dbManager, relation_name,
				sortingCols);
		Relation relation = dbManager.schema_manager.getRelation(relation_name);
		ArrayList<TupleObject> tupleObjectArray = new ArrayList<>();

		int relationTempSize = relation.getNumOfBlocks();
		int currRelationIdx = 0;

		while (relationTempSize > currRelationIdx) {
			relation.getBlock(currRelationIdx, 0);
			Block mb = dbManager.mem.getBlock(0);
			ArrayList<Tuple> blockTuples = mb.getTuples();
			GeneralUtils.addTuplesToArray(tupleObjectArray, blockTuples,
					sortingCols);
			currRelationIdx++;
		}

		// Sort the array
		Collections.sort(tupleObjectArray);

		for (TupleObject tupleObject : tupleObjectArray) {
			if (next_operator != null) {
				((ProjectionOperator) next_operator)
						.printTuple(tupleObject.tuple, printResult);
			} else if (printResult) {
				System.out.println(tupleObject.tuple);
				writer.println(tupleObject.tuple);
			}
			res_tuples.add(tupleObject.tuple);
		}
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
