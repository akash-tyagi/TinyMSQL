package database.physicalquery;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import database.DbManager;
import database.utils.GeneralUtils;
import database.utils.OnePassUtils;
import database.utils.TupleObject;
import database.utils.TwoPassUtils;
import java.util.HashMap;
import java.util.HashSet;
import storageManager.Block;
import storageManager.Field;
import storageManager.FieldType;
import storageManager.MainMemory;
import storageManager.Relation;
import storageManager.Schema;
import storageManager.Tuple;

public class JoinOperator extends OperatorBase implements OperatorInterface {
	String relation_name2;
	ArrayList<String> joinColumns;

	public JoinOperator(String rel1, String rel2, DbManager manager,
			PrintWriter writer, ArrayList<String> joinColumns) {
		super(manager, writer);
		relation_name = rel1;
		relation_name2 = rel2;
		this.joinColumns = joinColumns;
		this.dbManager = manager;
	}

	@Override
	public List<Tuple> execute(boolean printResult) {
		System.out.println("Trying one pass");
		Relation rel = onePassJoin(false);
		if (rel == null) {
			System.out.println("Two pass join");
			rel = twoPassJoin(false);
		}
		if (rel == null) {
			System.err.println("Simple Join");
			rel = simpleJoin(false);
		}
		return res_tuples;
	}

	private Relation onePassJoin(boolean storeOutputToDisk) {
		System.out.println(relation_name + "," + relation_name2);
		Relation r1 = dbManager.schema_manager.getRelation(relation_name);
		Relation r2 = dbManager.schema_manager.getRelation(relation_name2);

		ArrayList<String> r1_fields = r1.getSchema().getFieldNames();
		ArrayList<String> r2_fields = r2.getSchema().getFieldNames();

		if (joinColumns == null) {
			joinColumns = GeneralUtils.getCommmonCols(r1_fields, r2_fields);
		}

		int availableMemorySize;
		availableMemorySize = storeOutputToDisk
				? dbManager.mem.getMemorySize() - 2
				: dbManager.mem.getMemorySize() - 1;

		Relation firstRel, secondRel;

		if (r1.getNumOfBlocks() <= availableMemorySize) {
			firstRel = r1;
			secondRel = r2;
		} else if (r2.getNumOfBlocks() <= availableMemorySize) {
			firstRel = r2;
			secondRel = r1;
		} else {
			System.out.println("One pass fail");
			return null;
		}

		ArrayList<String> temp_field_names = new ArrayList<>();
		ArrayList<FieldType> temp_field_types = new ArrayList<>();
		GeneralUtils.updateInfoForTempSchema(firstRel, secondRel, joinColumns,
				temp_field_names, temp_field_types);

		// Copy first relation to memory
		int memIndex = 0;
		for (int i = 0; i < firstRel.getNumOfBlocks(); i++) {
			firstRel.getBlock(i, memIndex++);
		}

		// Sort first relation which is in memory
		GeneralUtils.sortMainMemory(dbManager, joinColumns,
				firstRel.getNumOfBlocks());

		// Temp relation for one-pass join
		Schema schema = new Schema(temp_field_names, temp_field_types);
		Relation one_pass_temp_relation = dbManager.schema_manager
				.createRelation(firstRel.getRelationName() + "_"
						+ secondRel.getRelationName(), schema);

		dbManager.temporaryCreatedRelations.add(
				firstRel.getRelationName() + "_" + secondRel.getRelationName());

		// One-pass algorithm
		for (int i = 0; i < secondRel.getNumOfBlocks(); i++) {
			secondRel.getBlock(i, memIndex);

			for (int j = 0; j < memIndex; j++) {
				OnePassUtils.joinBlocksData(dbManager.mem, firstRel, secondRel,
						dbManager.mem.getBlock(j),
						dbManager.mem.getBlock(memIndex),
						one_pass_temp_relation, storeOutputToDisk, joinColumns,
						memIndex);
			}
		}
		return one_pass_temp_relation;
	}

	private Relation twoPassJoin(boolean storeOutputToDisk) {
		Relation r1 = dbManager.schema_manager.getRelation(relation_name);
		Relation r2 = dbManager.schema_manager.getRelation(relation_name2);

		ArrayList<String> r1_fields = r1.getSchema().getFieldNames();
		ArrayList<String> r2_fields = r2.getSchema().getFieldNames();

		if (joinColumns == null) {
			joinColumns = GeneralUtils.getCommmonCols(r1_fields, r2_fields);
		}

		ArrayList<String> temp_field_names = new ArrayList<>();
		ArrayList<FieldType> temp_field_types = new ArrayList<>();
		GeneralUtils.updateInfoForTempSchema(r1, r2, joinColumns,
				temp_field_names, temp_field_types);

		// For 1 pass
		int availableMemorySize;
		availableMemorySize = storeOutputToDisk
				? dbManager.mem.getMemorySize() - 2
				: dbManager.mem.getMemorySize() - 1;

		if (r1.getNumOfBlocks() <= availableMemorySize
				|| r2.getNumOfBlocks() <= availableMemorySize) { // Use one pass
			// algo
			return OnePassUtils.onePassJoin(dbManager, relation_name,
					relation_name2, joinColumns, storeOutputToDisk);
		}

		int firstRelBlockReq;
		if ((r1.getNumOfBlocks() % dbManager.mem.getMemorySize()) == 0) {
			firstRelBlockReq = (r1.getNumOfBlocks()
					/ dbManager.mem.getMemorySize());
		} else {
			firstRelBlockReq = (r1.getNumOfBlocks()
					/ dbManager.mem.getMemorySize()) + 1;
		}

		int secondRelBlockReq;
		if ((r2.getNumOfBlocks() % dbManager.mem.getMemorySize()) == 0) {
			secondRelBlockReq = (r2.getNumOfBlocks()
					/ dbManager.mem.getMemorySize());
		} else {
			secondRelBlockReq = (r2.getNumOfBlocks()
					/ dbManager.mem.getMemorySize()) + 1;
		}

		int minMemoryNeeded = firstRelBlockReq + secondRelBlockReq;
		if (storeOutputToDisk) {
			minMemoryNeeded = minMemoryNeeded + 1;
		}

		if (minMemoryNeeded > dbManager.mem.getMemorySize()) {
			return null; // memory not enough for two pass algos
		}
                
                int surplusBlockCount = dbManager.mem.getMemorySize() - minMemoryNeeded;
                
                
                if(joinColumns.size() == 1 && dbManager.vTable.containsKey(relation_name) && dbManager.vTable.containsKey(relation_name2)){    
                    String joinColumn = joinColumns.get(0);
                    
                    //HashSet<String> hs1 = new HashSet<>();
                    //HashSet<String> hs2 = new HashSet<>();
                    
                    HashSet<String> hs = new HashSet<>(dbManager.vTable.get(relation_name).get(joinColumn).keySet());
                    HashSet<String> hs2 = new HashSet<>(dbManager.vTable.get(relation_name2).get(joinColumn).keySet());
                    
                    HashSet<String> commonVals = new HashSet<>();
                    
                    for(String val : hs){
                        if(hs2.contains(val)){
                            commonVals.add(val);
                        }
                    }
                    
                    for(String val : commonVals){
                        Integer count = dbManager.vTable.get(relation_name).get(joinColumn).get(val);
                        Integer count2 = dbManager.vTable.get(relation_name2).get(joinColumn).get(val);
                        
			int surplusBlocksReqRel1;
			int tuplesPerBlockForRel1 = 8 / dbManager.schema_manager.getRelation(relation_name).getSchema().getNumOfFields();
					/// tupleObjectArray1.get(0).tuple.getNumOfFields();
			if (count % tuplesPerBlockForRel1 == 0) {
				surplusBlocksReqRel1 = count / tuplesPerBlockForRel1;
			} else {
				surplusBlocksReqRel1 = (count / tuplesPerBlockForRel1)
						+ 1;
			}

			int surplusBlocksReqRel2;
			int tuplesPerBlockForRel2 = 8 / dbManager.schema_manager.getRelation(relation_name2).getSchema().getNumOfFields();
					/// tupleObjectArray2.get(0).tuple.getNumOfFields();
			if (count2 % tuplesPerBlockForRel2 == 0) {
				surplusBlocksReqRel2 = count2 / tuplesPerBlockForRel2;
			} else {
				surplusBlocksReqRel2 = (count2 / tuplesPerBlockForRel2)
						+ 1;
			}

			if (surplusBlocksReqRel1
					+ surplusBlocksReqRel2 > surplusBlockCount) {
				return null;
			}                        
                    }
                    
                }
                
		// Temp relation for two-pass join
		Schema schema = new Schema(temp_field_names, temp_field_types);
		Relation two_pass_temp_relation = dbManager.schema_manager
				.createRelation(
						r1.getRelationName() + "_" + r2.getRelationName(),
						schema);

		TwoPassUtils.sortRelationForPhase1(dbManager, relation_name,
				joinColumns);
		TwoPassUtils.sortRelationForPhase1(dbManager, relation_name2,
				joinColumns);

		// TODO start
		// Check number of blocks left (total - (firstRelBlockReq +
		// secondRelBlockReq + 1))
		// Check if extras can be adjusted in them

		// Read tuples to temp array for relation r1
		ArrayList<TupleObject> tupleObjectArray1 = new ArrayList<>();

		int relationTempSize = r1.getNumOfBlocks();
		int currRelationIdx = 0;

		while (relationTempSize > currRelationIdx) {
			r1.getBlock(currRelationIdx, 0);
			Block mb = dbManager.mem.getBlock(0);
			ArrayList<Tuple> blockTuples = mb.getTuples();
			GeneralUtils.addTuplesToArray(tupleObjectArray1, blockTuples,
					joinColumns);
			currRelationIdx++;
		}
		System.out.println(tupleObjectArray1.size());

		// Sort the array
		Collections.sort(tupleObjectArray1);
		// Read tuples to temp array for relation r2
		ArrayList<TupleObject> tupleObjectArray2 = new ArrayList<>();

		relationTempSize = r2.getNumOfBlocks();
		currRelationIdx = 0;

		while (relationTempSize > currRelationIdx) {
			r2.getBlock(currRelationIdx, 0);
			Block mb = dbManager.mem.getBlock(0);
			ArrayList<Tuple> blockTuples = mb.getTuples();
			GeneralUtils.addTuplesToArray(tupleObjectArray2, blockTuples,
					joinColumns);
			currRelationIdx++;
		}
		System.out.println(tupleObjectArray2.size());

		// Sort the array
		Collections.sort(tupleObjectArray2);

		int idx1 = 0;
		int idx2 = 0;

		while (idx1 < tupleObjectArray1.size()
				&& idx2 < tupleObjectArray2.size()) {

			ArrayList<TupleObject> tempTOA1 = new ArrayList<>();
			ArrayList<TupleObject> tempTOA2 = new ArrayList<>();

			if (GeneralUtils.projectedColumnsMatch(
					tupleObjectArray1.get(idx1).tuple,
					tupleObjectArray2.get(idx2).tuple, joinColumns)) {
				Tuple tMatched = tupleObjectArray1.get(idx1).tuple;

				while (idx1 < tupleObjectArray1.size()
						&& GeneralUtils.projectedColumnsMatch(tMatched,
								tupleObjectArray1.get(idx1).tuple,
								joinColumns)) {

					tempTOA1.add(tupleObjectArray1.get(idx1));
					idx1++;
				}
				while (idx2 < tupleObjectArray2.size()
						&& GeneralUtils.projectedColumnsMatch(tMatched,
								tupleObjectArray2.get(idx2).tuple,
								joinColumns)) {
					tempTOA2.add(tupleObjectArray2.get(idx2));
					idx2++;
				}

			}

			// doJoin
			int surplusBlocksReqRel1;
			int tuplesPerBlockForRel1 = 8
					/ tupleObjectArray1.get(0).tuple.getNumOfFields();
			if (tempTOA1.size() % tuplesPerBlockForRel1 == 0) {
				surplusBlocksReqRel1 = tempTOA1.size() / tuplesPerBlockForRel1;
			} else {
				surplusBlocksReqRel1 = (tempTOA1.size() / tuplesPerBlockForRel1)
						+ 1;
			}

			int surplusBlocksReqRel2;
			int tuplesPerBlockForRel2 = 8
					/ tupleObjectArray2.get(0).tuple.getNumOfFields();
			if (tempTOA2.size() % tuplesPerBlockForRel2 == 0) {
				surplusBlocksReqRel2 = tempTOA2.size() / tuplesPerBlockForRel2;
			} else {
				surplusBlocksReqRel2 = (tempTOA2.size() / tuplesPerBlockForRel2)
						+ 1;
			}

			if (surplusBlocksReqRel1
					+ surplusBlocksReqRel2 > surplusBlockCount) {
				dbManager.schema_manager.deleteRelation(
						two_pass_temp_relation.getRelationName());
				dbManager.temporaryCreatedRelations
						.remove(two_pass_temp_relation.getRelationName());
				return null;
			}

			joinTOAData(dbManager.mem, r1, r2, tempTOA1, tempTOA2,
					two_pass_temp_relation, storeOutputToDisk, joinColumns);

			if (idx1 < tupleObjectArray1.size()
					&& idx2 < tupleObjectArray2.size()) {
				if (GeneralUtils.tupleBiggerThan(
						tupleObjectArray1.get(idx1).tuple,
						tupleObjectArray2.get(idx2).tuple, joinColumns) > 0) {
					idx2++;
				} else if (GeneralUtils.tupleBiggerThan(
						tupleObjectArray1.get(idx1).tuple,
						tupleObjectArray2.get(idx2).tuple, joinColumns) < 0) {
					idx1++;
				}
			}
		}

		return two_pass_temp_relation;
	}

	private void joinTOAData(MainMemory mem, Relation r1, Relation r2,
			ArrayList<TupleObject> tempTOA1, ArrayList<TupleObject> tempTOA2,
			Relation two_pass_temp_relation, boolean storeOutputToDisk,
			ArrayList<String> commonCols) {

		for (TupleObject to1 : tempTOA1) {
			for (TupleObject to2 : tempTOA2) {
				Tuple t1 = to1.tuple;
				Tuple t2 = to2.tuple;
				boolean toInclude = true;
				Tuple tuple = two_pass_temp_relation.createTuple();
				for (int i = 0; i < t1.getNumOfFields(); i++) {
					Field f1 = t1.getField(i);
					String field_name = t1.getSchema().getFieldName(i);
					if (commonCols.contains(field_name)) {
						Field f2 = t2.getField(field_name);
						if (f1.toString().equals(f2.toString())) {
							if (f1.type == FieldType.INT) {
								tuple.setField(r1.getRelationName() + "_"
										+ r2.getRelationName() + "."
										+ field_name, f1.integer);
							} else {
								tuple.setField(r1.getRelationName() + "_"
										+ r2.getRelationName() + "."
										+ field_name, f1.str);
							}
						} else {
							toInclude = false;
							break;
						}
					} else {
						if (f1.type == FieldType.INT) {
							tuple.setField(
									r1.getRelationName() + "." + field_name,
									f1.integer);
						} else {
							tuple.setField(
									r1.getRelationName() + "." + field_name,
									f1.str);
						}
					}
				}

				if (toInclude) {
					for (int i = 0; i < t2.getNumOfFields(); i++) {
						Field f2 = t2.getField(i);
						String field_name = t2.getSchema().getFieldName(i);
						if (!commonCols.contains(field_name)) {
							if (f2.type == FieldType.INT) {
								tuple.setField(
										r2.getRelationName() + "." + field_name,
										f2.integer);
							} else {
								tuple.setField(
										r2.getRelationName() + "." + field_name,
										f2.str);
							}
						}
					}

					if (storeOutputToDisk) {
						GeneralUtils.appendTupleToRelation(
								two_pass_temp_relation, mem,
								mem.getMemorySize() - 1, tuple);
					} else {
						System.out.println("TEST" + tuple);
					}

				}

			}
		}

	}

	private Relation simpleJoin(boolean storeOutputToDisk) {
		Relation r1 = dbManager.schema_manager.getRelation(relation_name);
		Relation r2 = dbManager.schema_manager.getRelation(relation_name2);

		ArrayList<String> r1_fields = r1.getSchema().getFieldNames();
		ArrayList<String> r2_fields = r2.getSchema().getFieldNames();

		if (joinColumns == null) {
			joinColumns = GeneralUtils.getCommmonCols(r1_fields, r2_fields);
		}

		ArrayList<String> temp_field_names = new ArrayList<>();
		ArrayList<FieldType> temp_field_types = new ArrayList<>();
		GeneralUtils.updateInfoForTempSchema(r1, r2, joinColumns,
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

				joinBlocksData(dbManager.mem, r1, r2, dbManager.mem.getBlock(0),
						dbManager.mem.getBlock(1), temp_relation,
						storeOutputToDisk, joinColumns,
						dbManager.mem.getMemorySize() - 1);

			}
		}

		return temp_relation;
	}

	public static void joinBlocksData(MainMemory mem, Relation r1, Relation r2,
			Block r1_block, Block r2_block, Relation one_pass_temp_relation,
			boolean storeOutputToDisk, ArrayList<String> commonCols,
			int usedMemIndex) {
		ArrayList<Tuple> r1_tuples = r1_block.getTuples();
		ArrayList<Tuple> r2_tuples = r2_block.getTuples();

		for (Tuple t1 : r1_tuples) {
			for (Tuple t2 : r2_tuples) {
				boolean toInclude = true;
				Tuple tuple = one_pass_temp_relation.createTuple();
				for (int i = 0; i < t1.getNumOfFields(); i++) {
					Field f1 = t1.getField(i);
					String field_name = t1.getSchema().getFieldName(i);
					if (commonCols.contains(field_name)) {
						Field f2 = t2.getField(field_name);
						if (f1.toString().equals(f2.toString())) {
							if (f1.type == FieldType.INT) {
								tuple.setField(r1.getRelationName() + "_"
										+ r2.getRelationName() + "."
										+ field_name, f1.integer);
							} else {
								tuple.setField(r1.getRelationName() + "_"
										+ r2.getRelationName() + "."
										+ field_name, f1.str);
							}
						} else {
							toInclude = false;
							break;
						}
					} else {
						if (f1.type == FieldType.INT) {
							tuple.setField(
									r1.getRelationName() + "." + field_name,
									f1.integer);
						} else {
							tuple.setField(
									r1.getRelationName() + "." + field_name,
									f1.str);
						}
					}
				}

				if (toInclude) {
					// System.out.println("ERE:"+t2.getSchema());
					for (int i = 0; i < t2.getNumOfFields(); i++) {
						Field f2 = t2.getField(i);
						String field_name = t2.getSchema().getFieldName(i);
						if (!commonCols.contains(field_name)) {
							if (f2.type == FieldType.INT) {
								tuple.setField(
										r2.getRelationName() + "." + field_name,
										f2.integer);
							} else {
								tuple.setField(
										r2.getRelationName() + "." + field_name,
										f2.str);
							}
						}
					}

					if (storeOutputToDisk) {
						GeneralUtils.appendTupleToRelation(
								one_pass_temp_relation, mem, usedMemIndex + 1,
								tuple);
					} else {
						System.out.println(tuple);
					}

				}

			}
		}
	}

	@Override
	public void setNextOperator(OperatorInterface operator) {
		this.next_operator = operator;
	}

}
