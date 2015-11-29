package database.utils;

import database.DbManager;
import java.util.ArrayList;
import storageManager.Block;
import storageManager.Field;
import storageManager.FieldType;
import storageManager.MainMemory;
import storageManager.Relation;
import storageManager.Schema;
import storageManager.Tuple;

public class OnePassUtils {

    static ArrayList<String> temp_field_names;
    static ArrayList<FieldType> temp_field_types;

    // Will return null if the memory is not enough for doing one-pass join on these two relations
    // Will return an empty relation if storeOutputToDisk is false
    // Will return a temp relation "join_relation_name1_relation_name2" with the join output if storeOutputToDisk is true
    public static Relation onePassJoin(DbManager dbManager,
            String relation_name1,
            String relation_name2,
            ArrayList<String> commonCols,
            boolean storeOutputToDisk) {
        Relation r1 = dbManager.schema_manager.getRelation(relation_name1);
        Relation r2 = dbManager.schema_manager.getRelation(relation_name2);

        ArrayList<String> r1_fields = r1.getSchema().getFieldNames();
        ArrayList<String> r2_fields = r2.getSchema().getFieldNames();

        if(commonCols == null){
            commonCols = getCommmonCols(r1_fields, r2_fields);
        }

        updateInfoForTempSchema(r1, r2);

        int availableMemorySize;
        availableMemorySize = storeOutputToDisk ? dbManager.mem.getMemorySize() - 2 : dbManager.mem.getMemorySize() - 1;

        Relation firstRel, secondRel;

        if (r1.getNumOfBlocks() <= availableMemorySize) {
            firstRel = r1;
            secondRel = r2;
        } else if (r2.getNumOfBlocks() <= availableMemorySize) {
            firstRel = r2;
            secondRel = r1;
        } else {
            return null;
        }

        // Copy first relation to memory
        int memIndex = 0;
        for (int i = 0; i < firstRel.getNumOfBlocks(); i++) {
            firstRel.getBlock(i, memIndex++);
        }

        // Sort first relation which is in memory
        GeneralUtils.sortMainMemory(dbManager, commonCols, firstRel.getNumOfBlocks());

        // Temp relation for one-pass join
        Schema schema = new Schema(temp_field_names, temp_field_types);
        Relation one_pass_temp_relation = dbManager.schema_manager.createRelation("join_" + relation_name1 + "_" + relation_name2, schema);

        // One-pass algorithm
        for (int i = 0; i < secondRel.getNumOfBlocks(); i++) {
            secondRel.getBlock(i, memIndex);

            for (int j = 0; j < memIndex; j++) {
                joinBlocksData(dbManager.mem, dbManager.mem.getBlock(j), dbManager.mem.getBlock(memIndex), one_pass_temp_relation, storeOutputToDisk, commonCols, memIndex);
            }
        }

        return one_pass_temp_relation;
    }

    public static ArrayList<String> getCommmonCols(ArrayList<String> r1_fields, ArrayList<String> r2_fields) {
        ArrayList<String> commonCols = new ArrayList<>(r1_fields);
        commonCols.retainAll(r2_fields);
        return commonCols;
    }

    private static void updateInfoForTempSchema(Relation r1, Relation r2) {
        temp_field_names = new ArrayList<>();
        temp_field_types = new ArrayList<>();

        for (String s : r1.getSchema().getFieldNames()) {
            temp_field_names.add(s);
            temp_field_types.add(r1.getSchema().getFieldType(s));
        }

        for (String s : r2.getSchema().getFieldNames()) {
            if (r1.getSchema().getFieldNames().contains(s)) {
                continue;  // don't add if already added in r1 loop
            }
            temp_field_names.add(s);
            temp_field_types.add(r2.getSchema().getFieldType(s));
        }
    }

    private static void joinBlocksData(MainMemory mem,
            Block r1_block,
            Block r2_block,
            Relation one_pass_temp_relation,
            boolean storeOutputToDisk,
            ArrayList<String> commonCols,
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
                                tuple.setField(field_name, f1.integer);
                            } else {
                                tuple.setField(field_name, f1.str);
                            }
                        } else {
                            toInclude = false;
                            break;
                        }
                    } else {
                        if (f1.type == FieldType.INT) {
                            tuple.setField(field_name, f1.integer);
                        } else {
                            tuple.setField(field_name, f1.str);
                        }
                    }
                }

                if (toInclude) {
                    for (int i = 0; i < t2.getNumOfFields(); i++) {
                        Field f2 = t2.getField(i);
                        String field_name = t2.getSchema().getFieldName(i);
                        if (!commonCols.contains(field_name)) {
                            if (f2.type == FieldType.INT) {
                                tuple.setField(field_name, f2.integer);
                            } else {
                                tuple.setField(field_name, f2.str);
                            }
                        }
                    }

                    if (storeOutputToDisk) {
                        GeneralUtils.appendTupleToRelation(one_pass_temp_relation, mem, usedMemIndex + 1, tuple);
                    } else {
                        System.out.println(tuple);
                    }

                }

            }
        }
    }
}
