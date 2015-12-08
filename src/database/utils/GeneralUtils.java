package database.utils;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import database.DbManager;
import database.GlobalVariable;
import storageManager.Block;
import storageManager.Field;
import storageManager.FieldType;
import storageManager.MainMemory;
import storageManager.Relation;
import storageManager.Schema;
import storageManager.Tuple;

public class GeneralUtils {

	static long startTime;

	public static void restartTimer(DbManager dbManager) {
		startTime = System.currentTimeMillis();
		dbManager.disk.resetDiskIOs();
		dbManager.disk.resetDiskTimer();
	}

	public static void cleanMainMemory(MainMemory mem) {
		for (int i = 0; i < mem.getMemorySize(); i++) {
			mem.getBlock(i).clear();
		}
	}

	public static void sortMainMemory(DbManager dbManager,
			List<String> sortingCols, int memBlocksUsed) {
		// We are assuming no holes in the relation
		// We are invalidating and refilling the last block used because
		// we don't know how much of the last block has been actually used ->
		// Probably we won't need this logic

		// Read tuples to temp array
		ArrayList<TupleObject> tupleObjectArray = new ArrayList<>();
		for (int i = 0; i < memBlocksUsed; i++) {
			Block mb = dbManager.mem.getBlock(i);
			ArrayList<Tuple> blockTuples = mb.getTuples();
			addTuplesToArray(tupleObjectArray, blockTuples, sortingCols);
		}
		// Sort the array
		Collections.sort(tupleObjectArray);
		// Write the sorted data back to memory blocks
		ArrayList<Tuple> sortedTuples = new ArrayList<>();
		for (TupleObject to : tupleObjectArray) {
			sortedTuples.add(to.tuple);
		}
		dbManager.mem.setTuples(0, sortedTuples);
	}

	public static void addTuplesToArray(List<TupleObject> tupleObjectArray,
			List<Tuple> blockTuples, List<String> sortingCols) {
		for (Tuple tuple : blockTuples) {
			ArrayList<String> colValues = new ArrayList<>();
			for (String col : sortingCols) {
				String s1 = col;
				for (String fn : tuple.getSchema().getFieldNames()) {
					if (fn.endsWith("." + col)) {
						s1 = fn;
					}
				}
				colValues.add(tuple.getField(s1).toString());
			}
			tupleObjectArray.add(new TupleObject(tuple, colValues));
		}
	}

	public static void appendTupleToRelation(Relation relation_reference,
			MainMemory mem, int memory_block_index, Tuple tuple) {
		Block block_reference;
		if (relation_reference.getNumOfBlocks() == 0) {
			if (GlobalVariable.isTestExecution) {
				System.out.print("The relation is empty" + "\n");
				System.out.print("Get the handle to the memory block "
						+ memory_block_index + " and clear it" + "\n");
			}
			block_reference = mem.getBlock(memory_block_index);
			block_reference.clear(); // clear the block
			block_reference.appendTuple(tuple); // append the tuple
			if (GlobalVariable.isTestExecution) {
				System.out.print(
						"Write to the first block of the relation" + "\n");
			}
			relation_reference.setBlock(relation_reference.getNumOfBlocks(),
					memory_block_index);
		} else {
			if (GlobalVariable.isTestExecution) {
				System.out
						.print("Read the last block of the relation into memory block 5:"
								+ "\n");
			}
			relation_reference.getBlock(relation_reference.getNumOfBlocks() - 1,
					memory_block_index);
			block_reference = mem.getBlock(memory_block_index);

			if (block_reference.isFull()) {
				if (GlobalVariable.isTestExecution) {
					System.out
							.print("(The block is full: Clear the memory block and append the tuple)"
									+ "\n");
				}
				block_reference.clear(); // clear the block
				block_reference.appendTuple(tuple); // append the tuple
				if (GlobalVariable.isTestExecution) {
					System.out
							.print("Write to a new block at the end of the relation"
									+ "\n");
				}
				relation_reference.setBlock(relation_reference.getNumOfBlocks(),
						memory_block_index); // write back to the relation
			} else {
				if (GlobalVariable.isTestExecution) {
					System.out
							.print("(The block is not full: Append it directly)"
									+ "\n");
				}
				block_reference.appendTuple(tuple); // append the tuple
				if (GlobalVariable.isTestExecution) {
					System.out.print(
							"Write to the last block of the relation" + "\n");
				}
				relation_reference.setBlock(
						relation_reference.getNumOfBlocks() - 1,
						memory_block_index); // write back to the relation
			}
		}
	}

	public static void appendTuplesToRelation(Relation relation_reference,
			MainMemory mem, int memory_block_index, List<Tuple> tuples) {
		Block block_reference = mem.getBlock(memory_block_index);
		block_reference.clear();
		int i = 0, size = tuples.size();
		Tuple tuple = null;

		if (relation_reference.getNumOfBlocks() != 0) {
			System.out.println("INSIDE");
			relation_reference.getBlock(relation_reference.getNumOfBlocks() - 1,
					memory_block_index);
			while (block_reference.isFull() == false && i < size) {
				tuple = tuples.get(i++);
				System.out.println("FULL" + tuple);
				block_reference.appendTuple(tuple);
			}
			relation_reference.setBlock(relation_reference.getNumOfBlocks() - 1,
					memory_block_index);
			block_reference.clear();
		}
		System.out.println(relation_reference.getNumOfBlocks());
		while (i < size) {
			while (block_reference.isFull() == false && i < size) {
				tuple = tuples.get(i++);
				System.out.println("Appending:" + tuple);
				block_reference.appendTuple(tuple);
			}
			System.out.println(relation_reference.getNumOfBlocks());
			;
			relation_reference.setBlock(relation_reference.getNumOfBlocks(),
					memory_block_index);
			block_reference.clear();
		}
	}

	public static ArrayList<String> getCommmonCols(ArrayList<String> r1_fields,
			ArrayList<String> r2_fields) {
		ArrayList<String> commonCols = new ArrayList<>(r1_fields);
		commonCols.retainAll(r2_fields);
		return commonCols;
	}

	public static void updateInfoForTempSchema(Relation r1, Relation r2,
			ArrayList<String> commonCols, ArrayList<String> temp_field_names,
			ArrayList<FieldType> temp_field_types) {

		for (String s : r1.getSchema().getFieldNames()) {
			String s1 = s;
			if (s1.contains(".")) {
				s1 = s1.split("\\.")[1];
			}
			if (commonCols.contains(s1)) {
				continue; // don't add if in common list
			}
			temp_field_names.add(r1.getRelationName() + "." + s1);
			temp_field_types.add(r1.getSchema().getFieldType(s));
		}

		for (String s : commonCols) {
			temp_field_names.add(r1.getRelationName() + "_"
					+ r2.getRelationName() + "." + s);
			if (r1.getSchema().fieldNameExists(s)) {
				temp_field_types.add(r1.getSchema().getFieldType(s));
			} else {
				String s1 = "";
				for (String fn : r1.getSchema().getFieldNames()) {
					if (fn.endsWith("." + s)) {
						s1 = fn;
					}
				}
				temp_field_types.add(r1.getSchema().getFieldType(s1));
			}
		}

		for (String s : r2.getSchema().getFieldNames()) {
			String s1 = s;
			if (s1.contains(".")) {
				s1 = s1.split("\\.")[1];
			}
			if (commonCols.contains(s1)) {
				continue; // don't add if in common list
			}
			temp_field_names.add(r2.getRelationName() + "." + s1);
			temp_field_types.add(r2.getSchema().getFieldType(s));
		}
	}

	public static void printExecutionStats(DbManager dbManager,
			PrintWriter writer) {
		long elapsedTimeMillis = System.currentTimeMillis() - startTime;
		System.out.print(
				"Computer elapse time = " + elapsedTimeMillis + " ms" + "\n");
		System.out.print("Execution time: = " + dbManager.disk.getDiskTimer()
				+ " ms" + "\n");
		writer.println(
				"Execution time: = " + dbManager.disk.getDiskTimer() + " ms");
		System.out.print("Disk I/Os = " + dbManager.disk.getDiskIOs() + "\n");
		writer.println("Disk I/Os = " + dbManager.disk.getDiskIOs());
		dbManager.disk.resetDiskIOs();
		dbManager.disk.resetDiskTimer();
		restartTimer(dbManager);
	}

	// Will return null if the memory is not enough for doing one-pass join on
	// these two relations
	// Will return an empty relation if storeOutputToDisk is false
	// Will return a temp relation "relation_name1_relation_name2" with the join
	// output if storeOutputToDisk is true
	public static Relation simpleJoin(DbManager dbManager,
			String relation_name1, String relation_name2,
			ArrayList<String> commonCols, boolean storeOutputToDisk) {
		Relation r1 = dbManager.schema_manager.getRelation(relation_name1);
		Relation r2 = dbManager.schema_manager.getRelation(relation_name2);

		ArrayList<String> r1_fields = r1.getSchema().getFieldNames();
		ArrayList<String> r2_fields = r2.getSchema().getFieldNames();

		if (commonCols == null) {
			commonCols = GeneralUtils.getCommmonCols(r1_fields, r2_fields);
		}

		ArrayList<String> temp_field_names = new ArrayList<>();
		ArrayList<FieldType> temp_field_types = new ArrayList<>();
		GeneralUtils.updateInfoForTempSchema(r1, r2, commonCols,
				temp_field_names, temp_field_types);

		// Temp relation for join
		Schema schema = new Schema(temp_field_names, temp_field_types);
		Relation temp_relation = dbManager.schema_manager.createRelation(
				r1.getRelationName() + "_" + r2.getRelationName(), schema);

		dbManager.temporaryCreatedRelations
				.add(r1.getRelationName() + "_" + r2.getRelationName());

		// Copy first relations to memory block by block
		for (int i = 0; i < r1.getNumOfBlocks(); i++) {
			r1.getBlock(i, 0);
			// join algorithm

			for (int j = 0; j < r2.getNumOfBlocks(); j++) {
				r2.getBlock(j, 1);

				OnePassUtils.joinBlocksData(dbManager.mem, r1, r2,
						dbManager.mem.getBlock(0), dbManager.mem.getBlock(1),
						temp_relation, storeOutputToDisk, commonCols,
						dbManager.mem.getMemorySize() - 1);

			}
		}

		return temp_relation;
	}

	public static void destroyTempRelations(DbManager dbManager) {
		for (String old_temp_relation : dbManager.temporaryCreatedRelations) {
			dbManager.schema_manager.deleteRelation(old_temp_relation);
		}
		dbManager.temporaryCreatedRelations.clear();
	}

	public static String getSimpleCol(Tuple t, String col) {
		String s1 = col;
		for (String fn : t.getSchema().getFieldNames()) {
			if (fn.endsWith("." + col)) {
				s1 = fn;
			}
		}
		return s1;
	}

	public static boolean projectedColumnsMatch(Tuple t, Tuple t1,
			ArrayList<String> projectionCols) {

		if (projectionCols.isEmpty()) { // THis is for mimicing that all the
			// columns are projected columns
			return t.toString().equals(t1.toString());
		}

		for (String projectionCol : projectionCols) {
			// System.out.println("11111111");
			String s1 = getSimpleCol(t1, projectionCol);
			String str1 = t1.getField(s1).toString();
			// System.out.println("4333"+ str1+" "+s1+"
			// "+t1.getSchema().getFieldNames());
			s1 = getSimpleCol(t, projectionCol);
			String str2 = t.getField(s1).toString();
			// System.out.println("22222");
			if (!str1.equals(str2)) {
				return false;
			}
		}

		return true;
	}

	public static int tupleBiggerThan(Tuple t, Tuple t1,
			ArrayList<String> projectionCols) {

		// if (projectionCols.isEmpty()) {
		// return t.toString().equals(t1.toString());
		// }
		for (String projectionCol : projectionCols) {
			String projectionCol1 = getSimpleCol(t, projectionCol);
			Field f = t.getField(projectionCol1);
			String projectionCol2 = getSimpleCol(t1, projectionCol);
			Field f1 = t1.getField(projectionCol2);
			if (f.type == FieldType.INT) {
				if (f.integer != f1.integer) {
					return f.integer - f1.integer;
				}
			} else {
				if (!f.str.equals(f1.str)) {
					return f.str.compareTo(f1.str);
				}
			}
		}

		return 0;
	}

	public static boolean projectedColumnsDataExists(Tuple t,
			ArrayList<Tuple> myMap, ArrayList<String> projectionCols) {
		for (Tuple t1 : myMap) {
			if (projectedColumnsMatch(t, t1, projectionCols)) {
				return true;
			}
		}
		return false;
	}

}
