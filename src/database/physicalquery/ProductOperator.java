package database.physicalquery;

import java.util.ArrayList;
import java.util.List;

import database.DbManager;
import storageManager.Block;
import storageManager.FieldType;
import storageManager.Relation;
import storageManager.Schema;
import storageManager.Tuple;

public class ProductOperator extends OperatorBase implements OperatorInterface {
	String relation_name2;
	int block_for_reading_1 = 0;
	int block_for_reading_2 = 1;
	int block_for_writing = 2;

	public ProductOperator(DbManager manager, String rel1, String rel2) {
		super(manager);
		// TODO Auto-generated constructor stub
		relation_name = rel1;
		relation_name2 = rel2;
	}

	@Override
	public void execute() {
		Relation rel1 = dbManager.schema_manager.getRelation(relation_name);
		Relation rel2 = dbManager.schema_manager.getRelation(relation_name2);
		System.out.println(
				"PRODUCT BETWEEN:" + relation_name + " " + relation_name2);
		String rel = productOperation(rel1, rel2);
		if (next_operator != null) {
			next_operator.setRelationName(rel);
			next_operator.execute();
		}
	}

	public String productOperation(Relation rel1, Relation rel2) {
		String join_relation_name = rel1.getRelationName() + "_"
				+ rel2.getRelationName();
		List<Relation> relations = new ArrayList<Relation>();
		relations.add(rel1);
		relations.add(rel2);

		ArrayList<String> field_names = new ArrayList<String>();
		ArrayList<FieldType> field_types = new ArrayList<FieldType>();
		for (Relation relation : relations) {
			Schema schema = relation.getSchema();
			for (FieldType fieldType : schema.getFieldTypes()) {
				field_types.add(fieldType);
			}
			for (String fieldName : schema.getFieldNames()) {
				field_names.add(relation.getRelationName() + '.' + fieldName);
			}
		}
		Schema join_rel_schema = new Schema(field_names, field_types);
		Relation join_relation = dbManager.schema_manager
				.createRelation(join_relation_name, join_rel_schema);
		int total_tuples = 0;
		Tuple join_tuple;
		Block write_block_ref = dbManager.mem.getBlock(block_for_writing);
		for (int i = 0; i < rel1.getNumOfBlocks(); i++) {
			rel1.getBlock(i, block_for_reading_1);
			Block rel1_block = dbManager.mem.getBlock(block_for_reading_1);
			List<Tuple> rel1_tuples = rel1_block.getTuples();
			for (Tuple tuple1 : rel1_tuples) {
				for (int j = 0; j < rel2.getNumOfBlocks(); j++) {
					rel2.getBlock(j, block_for_reading_2);
					Block rel2_block = dbManager.mem
							.getBlock(block_for_reading_2);
					List<Tuple> rel2_tuples = rel2_block.getTuples();
					for (Tuple tuple2 : rel2_tuples) {
						join_tuple = join_relation.createTuple();
						List<Tuple> from = new ArrayList<Tuple>();
						from.add(tuple1);
						from.add(tuple2);
						copyTupleFields(from, join_tuple);
						total_tuples++;
						if (next_operator == null) {
							System.out.println(join_tuple.toString());
							continue;
						}
						while (!write_block_ref.appendTuple(join_tuple)) {
							join_relation.setBlock(
									join_relation.getNumOfBlocks(),
									block_for_writing);
							write_block_ref.clear();
						}
					}
				}
			}
		}
		// WRITE LAST BLOCK PENDING IN MEMORY If NOT EMPTY
		if (next_operator != null && !write_block_ref.isEmpty()) {
			join_relation.setBlock(join_relation.getNumOfBlocks(),
					block_for_writing);
			write_block_ref.clear();
		}
		System.out.println("PRODUCT:" + relation_name + " and " + relation_name2
				+ " tuples generated:" + total_tuples);
		return join_relation_name;
	}

	public void copyTupleFields(List<Tuple> from, Tuple to) {
		int i = 0;
		for (Tuple tuple : from)
			for (int offset = 0; offset < tuple.getNumOfFields(); offset++) {
				if (tuple.getField(offset).type == FieldType.INT) {
					to.setField(i, tuple.getField(offset).integer);
				} else {
					to.setField(i, tuple.getField(offset).str);
				}
				i++;
			}
	}

	private boolean callWhereCondition(Tuple tuple) {
		return true;
		// if (cond == null || cond.execute(tuple)) {
		// System.out.println(tuple.toString() + " SATISFIED");
		// return true;
		// } else {
		// System.out.println(tuple.toString() + " NOT");
		// return false;
		// }
	}

}
