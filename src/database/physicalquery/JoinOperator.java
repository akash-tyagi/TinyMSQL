package database.physicalquery;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import database.DbManager;
import database.utils.GeneralUtils;
import database.utils.OnePassUtils;
import storageManager.FieldType;
import storageManager.Relation;
import storageManager.Schema;
import storageManager.Tuple;

public class JoinOperator extends OperatorBase implements OperatorInterface {
	String relationName2;
	ArrayList<String> joinColumns;

	public JoinOperator(String rel1, String rel2, DbManager manager,
			PrintWriter writer, ArrayList<String> joinColumns) {
		super(manager, writer);
		relation_name = rel1;
		relationName2 = rel2;
		this.joinColumns = joinColumns;
		this.dbManager = manager;
	}

	@Override
	public List<Tuple> execute(boolean printResult) {
		System.out.println("EXECTUGIN");
		onePassJoin(dbManager, relation_name, relationName2, false);
		return res_tuples;
	}

	private Relation onePassJoin(DbManager dbManager, String relation_name1,
			String relation_name2, boolean storeOutputToDisk) {
		Relation r1 = dbManager.schema_manager.getRelation(relation_name1);
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
			return null;
		}

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
				.createRelation(
						r1.getRelationName() + "_" + r2.getRelationName(),
						schema);

		dbManager.temporaryCreatedRelations
				.add(r1.getRelationName() + "_" + r2.getRelationName());

		// One-pass algorithm
		for (int i = 0; i < secondRel.getNumOfBlocks(); i++) {
			secondRel.getBlock(i, memIndex);

			for (int j = 0; j < memIndex; j++) {
				OnePassUtils.joinBlocksData(dbManager.mem, r1, r2,
						dbManager.mem.getBlock(j),
						dbManager.mem.getBlock(memIndex),
						one_pass_temp_relation, storeOutputToDisk, joinColumns,
						memIndex);
			}
		}

		return one_pass_temp_relation;
	}

	@Override
	public void setNextOperator(OperatorInterface operator) {
		this.next_operator = operator;
	}

}
