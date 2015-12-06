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

public class DuplicateOperator extends OperatorBase
        implements OperatorInterface {

    String sorting_column_name;
    public List<String> selectList;

    public DuplicateOperator(DbManager dbManager, PrintWriter writer,
            String column_name, List<String> selectList) {
        super(dbManager, writer);
        this.sorting_column_name = column_name;
        this.selectList = selectList;
    }

    @Override
    public List<Tuple> execute(boolean printResult) {
        if (isReadFromMem) {
            System.out.println("IN MEM DUPLICATE");
            duplicateRemovalInMemory(printResult);
        } else {
            int blocks = dbManager.schema_manager.getRelation(relation_name)
                    .getNumOfBlocks();
            if (blocks <= GlobalVariable.TOTAL_DATA_BLOCKS) {
                System.out.println("CALLING ONE PASS DUPLICATE");
                onePassDuplicate(printResult);
            } else {
                System.out.println("TWO PASSSSSSS");
                duplicateRemovalDisk(printResult);
            }
        }
        return res_tuples;
    }

    private void duplicateRemovalInMemory(boolean printResult) {
        ArrayList<String> sortingCols = new ArrayList<>();
        if (sorting_column_name != null) {
            sortingCols.add(sorting_column_name);
        }

        for (String field_name : dbManager.mem.getBlock(start_block).getTuple(0)
                .getSchema().getFieldNames()) {
            if (!sortingCols.contains(field_name)) {
                sortingCols.add(field_name);
            }
        }
        ArrayList<TupleObject> tupleObjectArray = new ArrayList<>();
        int currBlockIdx = start_block;
        while (end_block >= currBlockIdx) {
            Block mb = dbManager.mem.getBlock(currBlockIdx);
            ArrayList<Tuple> blockTuples = mb.getTuples();
            GeneralUtils.addTuplesToArray(tupleObjectArray, blockTuples,
                    sortingCols);
            currBlockIdx++;
        }

        // Sort the array
        Collections.sort(tupleObjectArray);

        Tuple t = tupleObjectArray.get(0).tuple;
        sendTupleToProjection(printResult, t);
        for (TupleObject tupleObject : tupleObjectArray) {
            if (!t.toString().equals(tupleObject.tuple.toString())) {
                t = tupleObject.tuple;
                sendTupleToProjection(printResult, t);
            }
        }

    }

    private void duplicateRemovalDisk(boolean printResult) {
        ArrayList<String> columns = new ArrayList<>();
        if (sorting_column_name != null) {
            columns.add(sorting_column_name);
        }
        twoPassRemoveDuplicate(dbManager, columns, printResult);
    }

    private void twoPassRemoveDuplicate(DbManager dbManager,
            ArrayList<String> sortingCols, boolean printResult) {
        if (dbManager.schema_manager.getRelation(relation_name)
                .getNumOfBlocks() > dbManager.mem.getMemorySize()
                * dbManager.mem.getMemorySize()) {
            System.out.println(
                    "ERROR: DUPLICATE REMOVAL NOT POSSIBLE, RELATION TOO BIG");
            return;
        }

        ArrayList<String> projectionCols = (ArrayList<String>) sortingCols.clone();

		// Add the remaining columns of relation to sortingCols.
        // If sortingCols == null, add all the columns of the relation to
        // sortingCols
        for (String field_name : dbManager.schema_manager
                .getRelation(relation_name).getSchema().getFieldNames()) {
            if (!sortingCols.contains(field_name)) {
                sortingCols.add(field_name);
            }
        }

        Relation relation = dbManager.schema_manager.createRelation(
                "sorted_unique_temp_" + relation_name,
                dbManager.schema_manager.getSchema(relation_name));
		// Relation relation =
        // dbManager.schema_manager.getRelation(relation_name);
        dbManager.temporaryCreatedRelations
                .add("sorted_unique_temp_" + relation_name);

// NIKHIL HACK                
//		TwoPassUtils.sortAndRemoveDuplicateForPhase1(dbManager, relation_name,
//				sortingCols, relation);
        TwoPassUtils.sortAndRemoveDuplicateForPhase1(dbManager, relation_name,
                projectionCols, relation);

		// TwoPassUtils.sortRelationForPhase1(dbManager, relation_name,
        // sortingCols);
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

        Tuple t = tupleObjectArray.get(0).tuple;
        sendTupleToProjection(printResult, t);
        for (TupleObject tupleObject : tupleObjectArray) {
            if (!GeneralUtils.projectedColumnsMatch(t, tupleObject.tuple, projectionCols)) {
                //if (!t.toString().equals(tupleObject.tuple.toString())) {
                t = tupleObject.tuple;
                sendTupleToProjection(printResult, t);
            }
        }
    }

    private void sendTupleToProjection(boolean printResult, Tuple t) {
        if (next_operator != null) {
            ((ProjectionOperator) next_operator).printTuple(t, printResult);
        } else if (printResult) {
            System.out.println(t);
            writer.println(t);
        }
        res_tuples.add(t);
    }

    private void onePassDuplicate(boolean printResult) {
        ArrayList<String> columns = new ArrayList<String>();
        if (sorting_column_name != null) {
            columns.add(sorting_column_name);
        }
        onePassRemoveDuplicate(dbManager, columns, printResult);
    }

    private void onePassRemoveDuplicate(DbManager dbManager,
            ArrayList<String> sortingCols, boolean printResult) {

        if (dbManager.schema_manager.getRelation(relation_name)
                .getNumOfBlocks() > dbManager.mem.getMemorySize()) {
            System.out.println(
                    "ERROR: DUPLICATE REMOVAL NOT POSSIBLE, RELATION TOO BIG");
            return;
        }

        ArrayList<String> projectionCols = (ArrayList<String>) sortingCols.clone();

		// Add the remaining columns of relation to sortingCols.
        // If sortingCols == null, add all the columns of the relation to
        // sortingCols
        for (String field_name : dbManager.schema_manager
                .getRelation(relation_name).getSchema().getFieldNames()) {
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

        GeneralUtils.sortMainMemory(dbManager, sortingCols,
                relation.getNumOfBlocks());

        Tuple t = dbManager.mem.getBlock(0).getTuple(0);
        sendTupleToProjection(printResult, t);
        for (int i = 0; i < relation.getNumOfBlocks(); i++) {
            Block mb = dbManager.mem.getBlock(i);
            ArrayList<Tuple> blockTuples = mb.getTuples();

            for (Tuple t1 : blockTuples) {
                if (!GeneralUtils.projectedColumnsMatch(t, t1, projectionCols)) {
                    //if (!t.toString().equals(t1.toString())) {
                    t = t1;
                    sendTupleToProjection(printResult, t);

                }
            }
        }
    }

}
