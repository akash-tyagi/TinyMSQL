package database.physicalquery;

import java.util.ArrayList;
import java.util.List;

import database.DbManager;
import database.GlobalVariable;
import database.parser.searchcond.SearchCond;
import storageManager.Block;
import storageManager.FieldType;
import storageManager.Relation;
import storageManager.Schema;
import storageManager.Tuple;

public class SelectOperator extends OperatorBase implements OperatorInterface {
	OperatorInterface operator;
	SearchCond cond;
	String relation_name;
	final int BLOCK_FOR_READING = 9;
	final int BLOCK_FOR_WRITING = 0;

	public SelectOperator(DbManager manager, String relation_name,
			SearchCond cond) {
		super(manager);
		this.cond = cond;
		this.relation_name = relation_name;
	}

	// @Override
	public void execute() {
		// productOperation();
		/*
		 * Selection can be done in one pass or two pass if one pass then simply
		 * let the other operator know if two pass create another table and
		 * store there and let next operator know about this new table name AND
		 * LAST CASE if no next operator and only needs to print data
		 */
		Relation rel = dbManager.schema_manager.getRelation(relation_name);
		// code for storing all the results in main memory and pass on
		if (rel.getNumOfBlocks() <= GlobalVariable.USABLE_DATA_BLOCKS) {
			readIntoMemBlocks(rel);
		}
		// code to create new temp table and pass the table name to next
		// operator
		else {
			selectBlockByBlock(rel);
		}
	}

	// returns the last memory block address in memory, starting is 0
	private int readIntoMemBlocks(Relation rel) {
		Block write_block_ref = dbManager.mem.getBlock(BLOCK_FOR_WRITING);
		write_block_ref.clear();
		int lastWriteBlock = 0;

		Block read_block_ref = dbManager.mem.getBlock(BLOCK_FOR_READING);
		for (int i = 0; i < rel.getNumOfBlocks(); i++) {
			read_block_ref.clear();
			rel.getBlock(i, BLOCK_FOR_READING);
			// ONCE BLOCK IS READ, NEED TO REREAD IT
			read_block_ref = dbManager.mem.getBlock(BLOCK_FOR_READING);
			ArrayList<Tuple> tuples = dbManager.mem.getBlock(BLOCK_FOR_READING)
					.getTuples();
			for (Tuple tuple : tuples) {
				if (callWhereCondition(tuple) == false)
					continue;
				// IF ONLY NEED TO PRINT TUPLE, NO NEXT OPERATOR
				if (operator == null) {
					System.out.print(tuples.toString() + " RESULT\n");
					continue;
				}
				// STORE IN MEMORY FOR NEXT OPERATOR
				while (!write_block_ref.appendTuple(tuple)) {
					lastWriteBlock++;
					write_block_ref = dbManager.mem.getBlock(lastWriteBlock);
					write_block_ref.clear();
				}
			}
		}
		return lastWriteBlock;
	}

	private void selectBlockByBlock(Relation rel) {
		// CREATING TEMP TABLE FOR SELECTION RESULT
		String new_relation_name = "select_" + relation_name;
		Relation new_relation = dbManager.schema_manager.createRelation(
				new_relation_name,
				dbManager.schema_manager.getSchema(relation_name));
		Block write_block_ref = dbManager.mem.getBlock(BLOCK_FOR_WRITING);
		write_block_ref.clear();

		Block read_block_ref = dbManager.mem.getBlock(BLOCK_FOR_READING);
		for (int i = 0; i < rel.getNumOfBlocks(); i++) {
			read_block_ref.clear();
			rel.getBlock(i, BLOCK_FOR_READING);
			read_block_ref = dbManager.mem.getBlock(BLOCK_FOR_READING);
			ArrayList<Tuple> tuples = read_block_ref.getTuples();
			for (Tuple tuple : tuples) {
				if (callWhereCondition(tuple) == false)
					continue;
				// IF ONLY NEED TO PRINT TUPLE, NO NEXT OPERATOR
				if (operator == null) {
					System.out.print(tuples.toString() + "\n");
					continue;
				}
				// STORE IN TEMP TABLE FOR NEXT OPERATOR
				while (!write_block_ref.appendTuple(tuple)) {
					new_relation.setBlock(new_relation.getNumOfBlocks(),
							BLOCK_FOR_WRITING);
					write_block_ref.clear();
				}
			}
		}
		// WRITE LAST BLOCK PENDING IN MEMORY If NOT EMPTY
		if (operator != null && !write_block_ref.isEmpty()) {
			new_relation.setBlock(new_relation.getNumOfBlocks(),
					BLOCK_FOR_WRITING);
			write_block_ref.clear();
		}
	}

	private boolean callWhereCondition(Tuple tuple) {
		if (cond == null || cond.execute(tuple)) {
			// System.out.println(tuple.toString() + " SATISFIED");
			return true;
		} else {
			// System.out.println(tuple.toString() + " NOT");
			return false;
		}
	}
}
