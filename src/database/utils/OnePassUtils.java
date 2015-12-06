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

    // Will return null if the memory is not enough for doing one-pass join on these two relations
    // Will return an empty relation if storeOutputToDisk is false
    // Will return a temp relation "relation_name1_relation_name2" with the join output if storeOutputToDisk is true
    public static Relation onePassJoin(DbManager dbManager,
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
        Relation one_pass_temp_relation = dbManager.schema_manager.createRelation(r1.getRelationName() + "_" + r2.getRelationName(), schema);

        dbManager.temporaryCreatedRelations.add(r1.getRelationName() + "_" + r2.getRelationName());

        // One-pass algorithm
        for (int i = 0; i < secondRel.getNumOfBlocks(); i++) {
            secondRel.getBlock(i, memIndex);

            for (int j = 0; j < memIndex; j++) {
                joinBlocksData(dbManager.mem, r1, r2, dbManager.mem.getBlock(j), dbManager.mem.getBlock(memIndex), one_pass_temp_relation, storeOutputToDisk, commonCols, memIndex);
            }
        }

        return one_pass_temp_relation;
    }

    public static Relation onePassRemoveDuplicate(DbManager dbManager,
            String relation_name,
            ArrayList<String> sortingCols,
            boolean storeOutputToDisk) {

        if (storeOutputToDisk) {
            if (dbManager.schema_manager.getRelation(relation_name).getNumOfBlocks() >= dbManager.mem.getMemorySize()) {
                return null;
            }
        }

        if (!storeOutputToDisk) {
            if (dbManager.schema_manager.getRelation(relation_name).getNumOfBlocks() > dbManager.mem.getMemorySize()) {
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

        Relation relation = dbManager.schema_manager.getRelation(relation_name);
        // Copy relation to memory
        int memIndex = 0;
        for (int i = 0; i < relation.getNumOfBlocks(); i++) {
            relation.getBlock(i, memIndex++);
        }

        GeneralUtils.sortMainMemory(dbManager, sortingCols, relation.getNumOfBlocks());

        if (storeOutputToDisk) {
            Relation sorted_unique_relation = dbManager.schema_manager.createRelation("sorted_unique_" + relation_name, relation.getSchema());
            dbManager.temporaryCreatedRelations.add("sorted_unique_" + relation_name);
            Tuple t = dbManager.mem.getBlock(0).getTuple(0);
            GeneralUtils.appendTupleToRelation(sorted_unique_relation,
                    dbManager.mem, dbManager.mem.getMemorySize() - 1,
                    t);
            for (int i = 0; i < relation.getNumOfBlocks(); i++) {
                Block mb = dbManager.mem.getBlock(0);
                ArrayList<Tuple> blockTuples = mb.getTuples();

                for (Tuple t1 : blockTuples) {
                    if (!t.toString().equals(t1.toString())) {
                        t = t1;
                        GeneralUtils.appendTupleToRelation(sorted_unique_relation,
                                dbManager.mem, dbManager.mem.getMemorySize() - 1,
                                t);
                    }
                }
            }
            return sorted_unique_relation;
        } else {
            Tuple t = dbManager.mem.getBlock(0).getTuple(0);
            System.out.println(t);

            for (int i = 0; i < relation.getNumOfBlocks(); i++) {
                Block mb = dbManager.mem.getBlock(i);
                ArrayList<Tuple> blockTuples = mb.getTuples();

                for (Tuple t1 : blockTuples) {
                    if (!t.toString().equals(t1.toString())) {
                        t = t1;
                        System.out.println(t);
                    }
                }
            }
        }
        return null;
    }

    // Note : r1 and r2 are not directly related to r1_block and r2_block
    // r1 and r2 are used just to define the order in column names
    // r1_block and r2_block are related to firstRel and secondRel
    public static void joinBlocksData(MainMemory mem,
            Relation r1,
            Relation r2,
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
                                tuple.setField(r1.getRelationName() + "_" + r2.getRelationName() + "." + field_name, f1.integer);
                            } else {
                                tuple.setField(r1.getRelationName() + "_" + r2.getRelationName() + "." + field_name, f1.str);
                            }
                        } else {
                            toInclude = false;
                            break;
                        }
                    } else {
                        if (f1.type == FieldType.INT) {
                            tuple.setField(r1.getRelationName() + "." + field_name, f1.integer);
                        } else {
                            tuple.setField(r1.getRelationName() + "." + field_name, f1.str);
                        }
                    }
                }

                if (toInclude) {
                    for (int i = 0; i < t2.getNumOfFields(); i++) {
                        Field f2 = t2.getField(i);
                        String field_name = t2.getSchema().getFieldName(i);
                        if (!commonCols.contains(field_name)) {
                            if (f2.type == FieldType.INT) {
                                tuple.setField(r2.getRelationName() + "." + field_name, f2.integer);
                            } else {
                                tuple.setField(r2.getRelationName() + "." + field_name, f2.str);
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
