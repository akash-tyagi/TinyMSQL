package database.utils;

import database.DbManager;
import java.util.ArrayList;
import java.util.Collections;
import storageManager.Block;
import storageManager.FieldType;
import storageManager.MainMemory;
import storageManager.Relation;
import storageManager.Schema;
import storageManager.Tuple;

public class TwoPassUtils {

    // Will return null if the memory is not enough for doing two-pass join on these two relations
    // Will return an empty relation if storeOutputToDisk is false
    // Will return a temp relation "relation_name1_relation_name2" with the join output if storeOutputToDisk is true
    public static Relation twoPassJoin(DbManager dbManager,
            String relation_name1,
            String relation_name2,
            ArrayList<String> commonCols,
            boolean storeOutputToDisk) {
        Relation r1 = dbManager.schema_manager.getRelation(relation_name1);
        Relation r2 = dbManager.schema_manager.getRelation(relation_name2);

        ArrayList<String> r1_fields = r1.getSchema().getFieldNames();
        ArrayList<String> r2_fields = r2.getSchema().getFieldNames();

        if (commonCols == null) {
            commonCols = GeneralUtils.getCommmonCols(r1_fields, r2_fields);
        }

        ArrayList<String> temp_field_names = new ArrayList<>();
        ArrayList<FieldType> temp_field_types = new ArrayList<>();
        GeneralUtils.updateInfoForTempSchema(r1, r2, commonCols, temp_field_names, temp_field_types);

        int availableMemorySize;
        availableMemorySize = storeOutputToDisk ? dbManager.mem.getMemorySize() - 2 : dbManager.mem.getMemorySize() - 1;

        if (r1.getNumOfBlocks() <= availableMemorySize || r2.getNumOfBlocks() <= availableMemorySize) { // Use one pass algo
            return OnePassUtils.onePassJoin(dbManager, relation_name1, relation_name2, commonCols, storeOutputToDisk);
        }

        int firstRelBlockReq = r1.getNumOfBlocks() / dbManager.mem.getMemorySize();
        int secondRelBlockReq = r2.getNumOfBlocks() / dbManager.mem.getMemorySize();

        if (firstRelBlockReq + secondRelBlockReq + 1 > dbManager.mem.getMemorySize()) {
            return null;    // memory not enough for two pass algos
        }

        // Temp relation for two-pass join
        Schema schema = new Schema(temp_field_names, temp_field_types);
        Relation two_pass_temp_relation = dbManager.schema_manager.createRelation(r1.getRelationName() + "_" + r2.getRelationName(), schema);

        sortRelationForPhase1(dbManager, relation_name1, commonCols);
        sortRelationForPhase1(dbManager, relation_name2, commonCols);

        ArrayList<Integer> r1Idx = new ArrayList<>();
        ArrayList<Integer> r2Idx = new ArrayList<>();

        for (int i = 0; i < firstRelBlockReq; i++) {
            r1Idx.add(i * dbManager.mem.getMemorySize());
        }

        for (int i = 0; i < secondRelBlockReq; i++) {
            r2Idx.add(i * dbManager.mem.getMemorySize());
        }

        while (!relationIsTraversed(r1Idx, dbManager.mem, r1) && !relationIsTraversed(r2Idx, dbManager.mem, r2)) {

        }

        return two_pass_temp_relation;
    }

    public static Relation twoPassSort(DbManager dbManager,
            String relation_name,
            ArrayList<String> sortingCols,
            boolean storeOutputToDisk) {
        if (!storeOutputToDisk) {
            if (dbManager.schema_manager.getRelation(relation_name).getNumOfBlocks()
                    > dbManager.mem.getMemorySize() * dbManager.mem.getMemorySize()) {
                return null;
            }
        }

        if (storeOutputToDisk) {
            if (dbManager.schema_manager.getRelation(relation_name).getNumOfBlocks()
                    > dbManager.mem.getMemorySize() * (dbManager.mem.getMemorySize() - 1)) {
                return null;
            }
        }

        // "sortingCols == null" won't arise because we won't need to sort full relation without
        // an "ORDER BY column_name" statement -> check same point in below duplicate removal
        sortRelationForPhase1(dbManager, relation_name, sortingCols);
//        int availableMemorySize = storeOutputToDisk ? dbManager.mem.getMemorySize() - 1 : dbManager.mem.getMemorySize();

//        int numberOfSortedSublists = dbManager.schema_manager.getRelation(relation_name).getNumOfBlocks() % availableMemorySize == 0
//                ? dbManager.schema_manager.getRelation(relation_name).getNumOfBlocks() / availableMemorySize
//                : dbManager.schema_manager.getRelation(relation_name).getNumOfBlocks() / availableMemorySize + 1;
//        ArrayList<Integer> subListsBlocksCounter = new ArrayList<>();
//
//        for (int i = 1; i < numberOfSortedSublists; i++) {
//            subListsBlocksCounter.add(dbManager.mem.getMemorySize());
//        }
//        subListsBlocksCounter.add(dbManager.schema_manager.getRelation(relation_name).getNumOfBlocks() % dbManager.mem.getMemorySize());
//        boolean[] validMemBlocks = new boolean[numberOfSortedSublists];
        Relation relation = dbManager.schema_manager.getRelation(relation_name);
//        while (!allElementsZero(subListsBlocksCounter)) {
//            for (int i = 0; i < numberOfSortedSublists - 1; i++) {
//                if (subListsBlocksCounter.get(i) != 0) {
//                    validMemBlocks[i] = true;
//                    relation.getBlock(i * dbManager.mem.getMemorySize() + (dbManager.mem.getMemorySize() - subListsBlocksCounter.get(i)), i);
//                    subListsBlocksCounter.set(i, subListsBlocksCounter.get(i) - 1);
//                }
//            }
//            if (subListsBlocksCounter.get(numberOfSortedSublists - 1) != 0) {
//                validMemBlocks[numberOfSortedSublists - 1] = true;
//                relation.getBlock((numberOfSortedSublists - 1) * dbManager.mem.getMemorySize()
//                        + ((dbManager.schema_manager.getRelation(relation_name).getNumOfBlocks() % dbManager.mem.getMemorySize())
//                        - subListsBlocksCounter.get(numberOfSortedSublists - 1)), numberOfSortedSublists - 1);
//                subListsBlocksCounter.set(numberOfSortedSublists - 1, subListsBlocksCounter.get(numberOfSortedSublists - 1) - 1);
//            }
//            
////            ArrayList<LinkedList<TupleObject>> = new ArrayList<>();
//            
//        }

        // Read tuples to temp array
        ArrayList<TupleObject> tupleObjectArray = new ArrayList<>();

        int relationTempSize = relation.getNumOfBlocks();
        int currRelationIdx = 0;

        while (relationTempSize > currRelationIdx) {
//            int currBatchSize = relationTempSize > availableMemorySize ? availableMemorySize : relationTempSize;
//            relationTempSize = relationTempSize - availableMemorySize;
//            
//            for(int  i = 0; i < currBatchSize; i++){
//                
//            }
            relation.getBlock(currRelationIdx, 0);
            Block mb = dbManager.mem.getBlock(0);
            ArrayList<Tuple> blockTuples = mb.getTuples();
            GeneralUtils.addTuplesToArray(tupleObjectArray, blockTuples, sortingCols);
            currRelationIdx++;
        }

        // Sort the array
        Collections.sort(tupleObjectArray);

        if (storeOutputToDisk) {
            Relation sorted_relation = dbManager.schema_manager.createRelation("sorted_" + relation_name, relation.getSchema());
            dbManager.temporaryCreatedRelations.add("sorted_" + relation_name);
            for (TupleObject tupleObject : tupleObjectArray) {
                GeneralUtils.appendTupleToRelation(sorted_relation,
                        dbManager.mem, dbManager.mem.getMemorySize() - 1,
                        tupleObject.tuple);
            }
            return sorted_relation;
        } else {
            for (TupleObject tupleObject : tupleObjectArray) {
                System.out.println(tupleObject.tuple);
            }
        }

        return null;
    }

    // In case we need sorting only for duplicat removal i.e no ORDER BY i.e sortingCols is empty
    // don't pass sortingCols = null, else pass sortingCols = new ArrayList<String>();
    public static Relation twoPassRemoveDuplicate(DbManager dbManager,
            String relation_name,
            ArrayList<String> sortingCols,
            boolean storeOutputToDisk) {
        if (!storeOutputToDisk) {
            if (dbManager.schema_manager.getRelation(relation_name).getNumOfBlocks()
                    > dbManager.mem.getMemorySize() * dbManager.mem.getMemorySize()) {
                return null;
            }
        }

        if (storeOutputToDisk) {
            if (dbManager.schema_manager.getRelation(relation_name).getNumOfBlocks()
                    > dbManager.mem.getMemorySize() * (dbManager.mem.getMemorySize() - 1)) {
                return null;
            }
        }

        // Add the remaining columns of relation to sortingCols.
        // If sortingCols == null, add all the columns of the relation to sortingCols
        for (String field_name : dbManager.schema_manager.getRelation(relation_name).getSchema().getFieldNames()) {
            if (!sortingCols.contains(field_name)) {
                sortingCols.add(field_name);
            }
        }

        // Standard way
        // sortRelationForPhase1(dbManager, relation_name, sortingCols);
        // Relation relation = dbManager.schema_manager.getRelation(relation_name);
        // My Hack
        Relation relation = dbManager.schema_manager.createRelation("sorted_unique_temp_" + relation_name,
                dbManager.schema_manager.getSchema(relation_name));
        dbManager.temporaryCreatedRelations.add("sorted_unique_temp_" + relation_name);
        sortAndRemoveDuplicateForPhase1(dbManager, relation_name, sortingCols, relation);

        // Read tuples to temp array
        ArrayList<TupleObject> tupleObjectArray = new ArrayList<>();

        int relationTempSize = relation.getNumOfBlocks();
        int currRelationIdx = 0;

        while (relationTempSize > currRelationIdx) {
            relation.getBlock(currRelationIdx, 0);
            Block mb = dbManager.mem.getBlock(0);
            ArrayList<Tuple> blockTuples = mb.getTuples();
            GeneralUtils.addTuplesToArray(tupleObjectArray, blockTuples, sortingCols);
            currRelationIdx++;
        }

        // Sort the array
        Collections.sort(tupleObjectArray);

        if (storeOutputToDisk) {
            Relation sorted_unique_relation = dbManager.schema_manager.createRelation("sorted_unique_" + relation_name, relation.getSchema());
            dbManager.temporaryCreatedRelations.add("sorted_unique_" + relation_name);
            Tuple t = tupleObjectArray.get(0).tuple;
            GeneralUtils.appendTupleToRelation(sorted_unique_relation,
                    dbManager.mem, dbManager.mem.getMemorySize() - 1,
                    t);
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

    public static void sortRelationForPhase1(DbManager dbManager, String tableName, ArrayList<String> sortingCols) {
        int memSize = dbManager.mem.getMemorySize();
        Relation relation = dbManager.schema_manager.getRelation(tableName);

        int subListStart = 0;
        int subListEnd = (relation.getNumOfBlocks() - subListStart >= memSize) ? memSize : relation.getNumOfBlocks();

        while (subListStart < subListEnd) {
            relation.getBlocks(subListStart, 0, subListEnd - subListStart);
            GeneralUtils.sortMainMemory(dbManager, sortingCols, subListEnd - subListStart);
            relation.setBlocks(subListStart, 0, subListEnd - subListStart);
            subListStart = subListEnd;
            subListEnd = (relation.getNumOfBlocks() - subListEnd >= subListEnd + memSize) ? subListEnd + memSize : relation.getNumOfBlocks();
        }
    }

    public static void sortAndRemoveDuplicateForPhase1(DbManager dbManager, String tableName, ArrayList<String> sortingCols, Relation sorted_unique_temp_relation) {
        int memSize = dbManager.mem.getMemorySize() - 1; // Note this line is different from above function
        Relation relation = dbManager.schema_manager.getRelation(tableName);

        int subListStart = 0;
        int subListEnd = (relation.getNumOfBlocks() - subListStart >= memSize) ? memSize : relation.getNumOfBlocks();

        while (subListStart < subListEnd) {
            relation.getBlocks(subListStart, 0, subListEnd - subListStart);
            GeneralUtils.sortMainMemory(dbManager, sortingCols, subListEnd - subListStart);

            // relation.setBlocks(subListStart, 0, subListEnd - subListStart);
            Tuple t = dbManager.mem.getBlock(0).getTuple(0);
            GeneralUtils.appendTupleToRelation(sorted_unique_temp_relation, dbManager.mem, memSize - 1, t);
            for (int i = 0; i < subListEnd - subListStart; i++) {
                Block mb = dbManager.mem.getBlock(i);
                ArrayList<Tuple> blockTuples = mb.getTuples();
                for (Tuple blockTuple : blockTuples) {
                    if (!t.toString().equals(blockTuple.toString())) {
                        t = blockTuple;
                        GeneralUtils.appendTupleToRelation(sorted_unique_temp_relation, dbManager.mem, memSize - 1, t);
                    }
                }
            }
            subListStart = subListEnd;
            subListEnd = (relation.getNumOfBlocks() - subListEnd >= subListEnd + memSize) ? subListEnd + memSize : relation.getNumOfBlocks();
        }
    }

    private static boolean relationIsTraversed(ArrayList<Integer> rIdx, MainMemory mem, Relation r) {
        for (int i = 0; i < rIdx.size() - 1; i++) {
            if (rIdx.get(i) < (i + 1) * mem.getMemorySize()) {
                return false;
            }
        }

        return rIdx.get(rIdx.size() - 1) >= r.getNumOfBlocks();
    }

    private static boolean allElementsZero(ArrayList<Integer> subListsBlocksCounter) {
        for (Integer i : subListsBlocksCounter) {
            if (i != 0) {
                return false;
            }
        }
        return true;
    }

}
