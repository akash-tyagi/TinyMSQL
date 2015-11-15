package database.utils;

import database.DbManager;
import java.util.ArrayList;
import java.util.Collections;
import storageManager.Block;
import storageManager.Relation;
import storageManager.Tuple;

public class TwoPassUtils {

    public static void sortRelationForPhase1(DbManager dbManager, String tableName, String[] sortingCols) {
        int memSize = dbManager.mem.getMemorySize();
        Relation relation = dbManager.schema_manager.getRelation(tableName);

        int subListStart = 0;
        int subListEnd = (relation.getNumOfBlocks() - subListStart >= memSize) ? memSize : relation.getNumOfBlocks();

        while (subListStart < subListEnd) {
            relation.getBlocks(subListStart, 0, subListEnd - subListStart);
            sortMainMemory(dbManager, sortingCols, subListEnd - subListStart);
            relation.setBlocks(subListStart, 0, subListEnd - subListStart);
            subListStart = subListEnd;
            subListEnd = (relation.getNumOfBlocks() - subListEnd >= subListEnd + memSize) ? subListEnd + memSize : relation.getNumOfBlocks();
        }
    }

    private static void sortMainMemory(DbManager dbManager, String[] sortingCols, int memBlocksUsed) {
        // We are assuming no holes in the relation
        // We are invalidating and refilling the last block used because 
        // we don't know how much of the last block has been actually used -> Probably we won't need this logic

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

    private static void addTuplesToArray(ArrayList<TupleObject> tupleObjectArray, ArrayList<Tuple> blockTuples, String[] sortingCols) {
        for (Tuple tuple : blockTuples) {
            ArrayList<String> colValues = new ArrayList<>();
            for (String col : sortingCols) {
                colValues.add(tuple.getField(col).toString());
            }
            tupleObjectArray.add(new TupleObject(tuple, colValues));
        }
    }
}
