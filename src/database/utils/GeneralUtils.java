package database.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import database.DbManager;
import storageManager.Block;
import storageManager.FieldType;
import storageManager.MainMemory;
import storageManager.Relation;
import storageManager.Tuple;

public class GeneralUtils {

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
                colValues.add(tuple.getField(col).toString());
            }
            tupleObjectArray.add(new TupleObject(tuple, colValues));
        }
    }

    public static void appendTupleToRelation(Relation relation_reference,
            MainMemory mem, int memory_block_index, Tuple tuple) {
        Block block_reference;
        if (relation_reference.getNumOfBlocks() == 0) {
            System.out.print("The relation is empty" + "\n");
            System.out.print("Get the handle to the memory block "
                    + memory_block_index + " and clear it" + "\n");
            block_reference = mem.getBlock(memory_block_index);
            block_reference.clear(); // clear the block
            block_reference.appendTuple(tuple); // append the tuple
            System.out.print("Write to the first block of the relation" + "\n");
            relation_reference.setBlock(relation_reference.getNumOfBlocks(),
                    memory_block_index);
        } else {
            System.out
                    .print("Read the last block of the relation into memory block 5:"
                            + "\n");
            relation_reference.getBlock(relation_reference.getNumOfBlocks() - 1,
                    memory_block_index);
            block_reference = mem.getBlock(memory_block_index);

            if (block_reference.isFull()) {
                System.out
                        .print("(The block is full: Clear the memory block and append the tuple)"
                                + "\n");
                block_reference.clear(); // clear the block
                block_reference.appendTuple(tuple); // append the tuple
                System.out
                        .print("Write to a new block at the end of the relation"
                                + "\n");
                relation_reference.setBlock(relation_reference.getNumOfBlocks(),
                        memory_block_index); // write back to the relation
            } else {
                System.out.print(
                        "(The block is not full: Append it directly)" + "\n");
                block_reference.appendTuple(tuple); // append the tuple
                System.out.print(
                        "Write to the last block of the relation" + "\n");
                relation_reference.setBlock(
                        relation_reference.getNumOfBlocks() - 1,
                        memory_block_index); // write back to the relation
            }
        }
    }

    public static ArrayList<String> getCommmonCols(ArrayList<String> r1_fields, ArrayList<String> r2_fields) {
        ArrayList<String> commonCols = new ArrayList<>(r1_fields);
        commonCols.retainAll(r2_fields);
        return commonCols;
    }

    public static void updateInfoForTempSchema(Relation r1,
            Relation r2,
            ArrayList<String> commonCols,
            ArrayList<String> temp_field_names,
            ArrayList<FieldType> temp_field_types) {

        for (String s : r1.getSchema().getFieldNames()) {
            if (commonCols.contains(s)) {
                continue;  // don't add if in common list
            }
            temp_field_names.add(r1.getRelationName() + "." + s);
            temp_field_types.add(r1.getSchema().getFieldType(s));
        }

        for (String s : commonCols) {
            temp_field_names.add(r1.getRelationName() + "_" + r2.getRelationName() + "." + s);
            temp_field_types.add(r1.getSchema().getFieldType(s));  // we could have taken either of r1 OR r2
        }

        for (String s : r2.getSchema().getFieldNames()) {
            if (commonCols.contains(s)) {
                continue;  // don't add if in common list
            }
            temp_field_names.add(r2.getRelationName() + "." + s);
            temp_field_types.add(r2.getSchema().getFieldType(s));
        }
    }

}
