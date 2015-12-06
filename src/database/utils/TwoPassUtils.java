package database.utils;

import database.DbManager;
import database.physicalquery.ProjectionOperator;

import java.util.ArrayList;
import java.util.Collections;
import storageManager.Block;
import storageManager.Field;
import storageManager.FieldType;
import storageManager.MainMemory;
import storageManager.Relation;
import storageManager.Schema;
import storageManager.Tuple;

public class TwoPassUtils {

	// Will return null if the memory is not enough for doing two-pass join on
    // these two relations
    // Will return an empty relation if storeOutputToDisk is false
    // Will return a temp relation "relation_name1_relation_name2" with the join
    // output if storeOutputToDisk is true
    public static Relation twoPassJoin(DbManager dbManager,
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

        // For 1 pass
        int availableMemorySize;
        availableMemorySize = storeOutputToDisk
                ? dbManager.mem.getMemorySize() - 2
                : dbManager.mem.getMemorySize() - 1;

        if (r1.getNumOfBlocks() <= availableMemorySize
                || r2.getNumOfBlocks() <= availableMemorySize) { // Use one pass
            // algo
            return OnePassUtils.onePassJoin(dbManager, relation_name1,
                    relation_name2, commonCols, storeOutputToDisk);
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

        // Temp relation for two-pass join
        Schema schema = new Schema(temp_field_names, temp_field_types);
        Relation two_pass_temp_relation = dbManager.schema_manager
                .createRelation(
                        r1.getRelationName() + "_" + r2.getRelationName(),
                        schema);

        sortRelationForPhase1(dbManager, relation_name1, commonCols);
        sortRelationForPhase1(dbManager, relation_name2, commonCols);

		// TODO start
        // Check number of blocks left (total - (firstRelBlockReq +
        // secondRelBlockReq + 1))
        // Check if extras can be adjusted in them
        int surplusBlockCount = dbManager.mem.getMemorySize() - minMemoryNeeded;

        // Read tuples to temp array for relation r1
        ArrayList<TupleObject> tupleObjectArray1 = new ArrayList<>();

        int relationTempSize = r1.getNumOfBlocks();
        int currRelationIdx = 0;

        while (relationTempSize > currRelationIdx) {
            r1.getBlock(currRelationIdx, 0);
            Block mb = dbManager.mem.getBlock(0);
            ArrayList<Tuple> blockTuples = mb.getTuples();
            GeneralUtils.addTuplesToArray(tupleObjectArray1, blockTuples,
                    commonCols);
            currRelationIdx++;
        }

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
                    commonCols);
            currRelationIdx++;
        }

        // Sort the array
        Collections.sort(tupleObjectArray2);

        int idx1 = 0;
        int idx2 = 0;

        while (idx1 < tupleObjectArray1.size()
                && idx2 < tupleObjectArray2.size()) {
            ArrayList<TupleObject> tempTOA1 = new ArrayList<>();
            ArrayList<TupleObject> tempTOA2 = new ArrayList<>();

            while ((idx1 < tupleObjectArray1.size()
                    && idx2 < tupleObjectArray2.size())
                    && (tupleObjectArray1.get(idx1)
                    .compareTo(tupleObjectArray2.get(idx2)) == 0)) {
                tempTOA1.add(tupleObjectArray1.get(idx1));
                tempTOA2.add(tupleObjectArray2.get(idx2));
                idx1++;
                idx2++;
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
                return null;
            }

            joinTOAData(dbManager.mem, r1, r2, tempTOA1, tempTOA2,
                    two_pass_temp_relation, storeOutputToDisk, commonCols);

            if (idx1 < tupleObjectArray1.size()
                    && idx2 < tupleObjectArray2.size()) {
                if (tupleObjectArray1.get(idx1)
                        .compareTo(tupleObjectArray2.get(idx2)) > 0) {
                    idx2++;
                } else if (tupleObjectArray1.get(idx1)
                        .compareTo(tupleObjectArray2.get(idx2)) < 0) {
                    idx1++;
                }
            }
        }

		// Also implement normal join - simple
        // ArrayList<Integer> r1Idx = new ArrayList<>();
        // ArrayList<Integer> r2Idx = new ArrayList<>();
        //
        // for (int i = 0; i < firstRelBlockReq; i++) {
        // r1Idx.add(i * dbManager.mem.getMemorySize());
        // }
        //
        // for (int i = 0; i < secondRelBlockReq; i++) {
        // r2Idx.add(i * dbManager.mem.getMemorySize());
        // }
        //
        // while (!relationIsTraversed(r1Idx, dbManager.mem, r1) &&
        // !relationIsTraversed(r2Idx, dbManager.mem, r2)) {
        //
        // }
        return two_pass_temp_relation;
    }

    public static Relation twoPassSort(DbManager dbManager,
            String relation_name, ArrayList<String> sortingCols,
            boolean storeOutputToDisk, ProjectionOperator projectionOperator,
            boolean printResult) {
        if (!storeOutputToDisk) {
            if (dbManager.schema_manager.getRelation(relation_name)
                    .getNumOfBlocks() > dbManager.mem.getMemorySize()
                    * dbManager.mem.getMemorySize()) {
                return null;
            }
        }

        if (storeOutputToDisk) {
            if (dbManager.schema_manager.getRelation(relation_name)
                    .getNumOfBlocks() > dbManager.mem.getMemorySize()
                    * (dbManager.mem.getMemorySize() - 1)) {
                return null;
            }
        }

		// "sortingCols == null" won't arise because we won't need to sort full
        // relation without
        // an "ORDER BY column_name" statement -> check same point in below
        // duplicate removal
        sortRelationForPhase1(dbManager, relation_name, sortingCols);
		// int availableMemorySize = storeOutputToDisk ?
        // dbManager.mem.getMemorySize() - 1 : dbManager.mem.getMemorySize();

		// int numberOfSortedSublists =
        // dbManager.schema_manager.getRelation(relation_name).getNumOfBlocks()
        // % availableMemorySize == 0
        // ?
        // dbManager.schema_manager.getRelation(relation_name).getNumOfBlocks()
        // / availableMemorySize
        // :
        // dbManager.schema_manager.getRelation(relation_name).getNumOfBlocks()
        // / availableMemorySize + 1;
        // ArrayList<Integer> subListsBlocksCounter = new ArrayList<>();
        //
        // for (int i = 1; i < numberOfSortedSublists; i++) {
        // subListsBlocksCounter.add(dbManager.mem.getMemorySize());
        // }
        // subListsBlocksCounter.add(dbManager.schema_manager.getRelation(relation_name).getNumOfBlocks()
        // % dbManager.mem.getMemorySize());
        // boolean[] validMemBlocks = new boolean[numberOfSortedSublists];
        Relation relation = dbManager.schema_manager.getRelation(relation_name);
		// while (!allElementsZero(subListsBlocksCounter)) {
        // for (int i = 0; i < numberOfSortedSublists - 1; i++) {
        // if (subListsBlocksCounter.get(i) != 0) {
        // validMemBlocks[i] = true;
        // relation.getBlock(i * dbManager.mem.getMemorySize() +
        // (dbManager.mem.getMemorySize() - subListsBlocksCounter.get(i)), i);
        // subListsBlocksCounter.set(i, subListsBlocksCounter.get(i) - 1);
        // }
        // }
        // if (subListsBlocksCounter.get(numberOfSortedSublists - 1) != 0) {
        // validMemBlocks[numberOfSortedSublists - 1] = true;
        // relation.getBlock((numberOfSortedSublists - 1) *
        // dbManager.mem.getMemorySize()
        // +
        // ((dbManager.schema_manager.getRelation(relation_name).getNumOfBlocks()
        // % dbManager.mem.getMemorySize())
        // - subListsBlocksCounter.get(numberOfSortedSublists - 1)),
        // numberOfSortedSublists - 1);
        // subListsBlocksCounter.set(numberOfSortedSublists - 1,
        // subListsBlocksCounter.get(numberOfSortedSublists - 1) - 1);
        // }
        //
        //// ArrayList<LinkedList<TupleObject>> = new ArrayList<>();
        //
        // }

        // Read tuples to temp array
        ArrayList<TupleObject> tupleObjectArray = new ArrayList<>();

        int relationTempSize = relation.getNumOfBlocks();
        int currRelationIdx = 0;

        while (relationTempSize > currRelationIdx) {
			// int currBatchSize = relationTempSize > availableMemorySize ?
            // availableMemorySize : relationTempSize;
            // relationTempSize = relationTempSize - availableMemorySize;
            //
            // for(int i = 0; i < currBatchSize; i++){
            //
            // }
            relation.getBlock(currRelationIdx, 0);
            Block mb = dbManager.mem.getBlock(0);
            ArrayList<Tuple> blockTuples = mb.getTuples();
            GeneralUtils.addTuplesToArray(tupleObjectArray, blockTuples,
                    sortingCols);
            currRelationIdx++;
        }

        // Sort the array
        Collections.sort(tupleObjectArray);

        if (storeOutputToDisk) {
            Relation sorted_relation = dbManager.schema_manager.createRelation(
                    "sorted_" + relation_name, relation.getSchema());
            dbManager.temporaryCreatedRelations.add("sorted_" + relation_name);
            for (TupleObject tupleObject : tupleObjectArray) {
                GeneralUtils.appendTupleToRelation(sorted_relation,
                        dbManager.mem, dbManager.mem.getMemorySize() - 1,
                        tupleObject.tuple);
            }
            return sorted_relation;
        } else {
            for (TupleObject tupleObject : tupleObjectArray) {
                if (projectionOperator != null) {
                    projectionOperator.printTuple(tupleObject.tuple,
                            printResult);
                } else if (printResult) {
                    System.out.println(tupleObject.tuple);
                }
            }
        }

        return null;
    }

	// In case we need sorting only for duplicate removal i.e no ORDER BY i.e
    // sortingCols is empty
    // don't pass sortingCols = null, instead, pass sortingCols = new
    // ArrayList<String>();
    public static Relation twoPassRemoveDuplicate(DbManager dbManager,
            String relation_name, ArrayList<String> sortingCols,
            boolean storeOutputToDisk) {
        if (!storeOutputToDisk) {
            if (dbManager.schema_manager.getRelation(relation_name)
                    .getNumOfBlocks() > dbManager.mem.getMemorySize()
                    * dbManager.mem.getMemorySize()) {
                return null;
            }
        }

        if (storeOutputToDisk) {
            if (dbManager.schema_manager.getRelation(relation_name)
                    .getNumOfBlocks() > dbManager.mem.getMemorySize()
                    * (dbManager.mem.getMemorySize() - 1)) {
                return null;
            }
        }

		// Add the remaining columns of relation to sortingCols.
        // If sortingCols == null, add all the columns of the relation to
        // sortingCols
        for (String field_name : dbManager.schema_manager
                .getRelation(relation_name).getSchema().getFieldNames()) {
            if (!sortingCols.contains(field_name)) {
                sortingCols.add(field_name);
            }
        }

		// Standard way
        // sortRelationForPhase1(dbManager, relation_name, sortingCols);
        // Relation relation =
        // dbManager.schema_manager.getRelation(relation_name);
        // My Hack
        Relation relation = dbManager.schema_manager.createRelation(
                "sorted_unique_temp_" + relation_name,
                dbManager.schema_manager.getSchema(relation_name));
        dbManager.temporaryCreatedRelations
                .add("sorted_unique_temp_" + relation_name);
        sortAndRemoveDuplicateForPhase1(dbManager, relation_name, sortingCols,
                relation);

        // Read tuples to temp array
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

        if (storeOutputToDisk) {
            Relation sorted_unique_relation = dbManager.schema_manager
                    .createRelation("sorted_unique_" + relation_name,
                            relation.getSchema());
            dbManager.temporaryCreatedRelations
                    .add("sorted_unique_" + relation_name);
            Tuple t = tupleObjectArray.get(0).tuple;
            GeneralUtils.appendTupleToRelation(sorted_unique_relation,
                    dbManager.mem, dbManager.mem.getMemorySize() - 1, t);
            for (TupleObject tupleObject : tupleObjectArray) {
                if (!t.toString().equals(tupleObject.tuple.toString())) {
                    t = tupleObject.tuple;
                    GeneralUtils.appendTupleToRelation(sorted_unique_relation,
                            dbManager.mem, dbManager.mem.getMemorySize() - 1,
                            t);
                }
            }
            return sorted_unique_relation;
        } else {
            Tuple t = tupleObjectArray.get(0).tuple;
            System.out.println(t);
            for (TupleObject tupleObject : tupleObjectArray) {
                if (!t.toString().equals(tupleObject.tuple.toString())) {
                    t = tupleObject.tuple;
                    System.out.println(t);
                }
            }
        }

        return null;
    }

    public static void sortRelationForPhase1(DbManager dbManager,
            String tableName, ArrayList<String> sortingCols) {
        int memSize = dbManager.mem.getMemorySize();
        Relation relation = dbManager.schema_manager.getRelation(tableName);

        int subListStart = 0;
        int subListEnd = (relation.getNumOfBlocks() - subListStart >= memSize)
                ? memSize : relation.getNumOfBlocks();

        while (subListStart < subListEnd) {
            relation.getBlocks(subListStart, 0, subListEnd - subListStart);
            GeneralUtils.sortMainMemory(dbManager, sortingCols,
                    subListEnd - subListStart);
            relation.setBlocks(subListStart, 0, subListEnd - subListStart);
            subListStart = subListEnd;
            subListEnd = (relation.getNumOfBlocks() - subListEnd >= memSize)
                    ? subListEnd + memSize : relation.getNumOfBlocks();
        }
    }

    public static void sortAndRemoveDuplicateForPhase1(DbManager dbManager,
            String tableName, ArrayList<String> sortingCols,
            Relation sorted_unique_temp_relation) {
        int memSize = dbManager.mem.getMemorySize() - 1; // Note this line is
        // different from
        // above function
        Relation relation = dbManager.schema_manager.getRelation(tableName);

        int subListStart = 0;
        int subListEnd = (relation.getNumOfBlocks() - subListStart >= memSize)
                ? memSize : relation.getNumOfBlocks();
        while (subListStart < subListEnd) {
            relation.getBlocks(subListStart, 0, subListEnd - subListStart);
            GeneralUtils.sortMainMemory(dbManager, sortingCols,
                    subListEnd - subListStart);

            // relation.setBlocks(subListStart, 0, subListEnd - subListStart);
            Tuple t = dbManager.mem.getBlock(0).getTuple(0);
            GeneralUtils.appendTupleToRelation(sorted_unique_temp_relation,
                    dbManager.mem, memSize, t);
            for (int i = 0; i < subListEnd - subListStart; i++) {
                Block mb = dbManager.mem.getBlock(i);
                ArrayList<Tuple> blockTuples = mb.getTuples();
                for (Tuple blockTuple : blockTuples) {
                    if (!GeneralUtils.projectedColumnsMatch(t, blockTuple, sortingCols)) {
                        //if (!t.toString().equals(blockTuple.toString())) {
                        t = blockTuple;
                        GeneralUtils.appendTupleToRelation(
                                sorted_unique_temp_relation, dbManager.mem,
                                memSize, t);
                    }
                }
            }
            subListStart = subListEnd;
            subListEnd = (relation.getNumOfBlocks() - subListEnd >= memSize)
                    ? subListEnd + memSize : relation.getNumOfBlocks();
        }
    }

    private static boolean relationIsTraversed(ArrayList<Integer> rIdx,
            MainMemory mem, Relation r) {
        for (int i = 0; i < rIdx.size() - 1; i++) {
            if (rIdx.get(i) < (i + 1) * mem.getMemorySize()) {
                return false;
            }
        }

        return rIdx.get(rIdx.size() - 1) >= r.getNumOfBlocks();
    }

    private static boolean allElementsZero(
            ArrayList<Integer> subListsBlocksCounter) {
        for (Integer i : subListsBlocksCounter) {
            if (i != 0) {
                return false;
            }
        }
        return true;
    }

    private static void joinTOAData(MainMemory mem, Relation r1, Relation r2,
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
                        System.out.println(tuple);
                    }

                }

            }
        }

    }

}
