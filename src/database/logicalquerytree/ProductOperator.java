package database.logicalquerytree;

import java.util.ArrayList;
import java.util.List;

import database.DbManager;
import database.parser.SelectStmt;
import storageManager.FieldType;
import storageManager.Relation;
import storageManager.Schema;
import storageManager.Tuple;

public class ProductOperator {
	List<String> tableList;
	DbManager dbManager;

	public ProductOperator(SelectStmt stmt) {
		this.tableList = stmt.tableList;
	}

	public void execute(SelectStmt stmt) {

	}

	public void joinOperation() {
		// Creating a single super schema with combined schema of all the tables
		// and combining tuples into one large tuple for where condition

		List<Relation> relations = new ArrayList<Relation>();
		for (String relation_name : tableList) {
			relations.add(dbManager.schema_manager.getRelation(relation_name));
		}

		// Creating super schema
		System.out.print("SELECT: Creating a Super schema" + "\n");
		Schema super_schema = getSuperSchema(relations, true);

		Relation super_relation = dbManager.schema_manager
				.createRelation("SuperRelation", super_schema);
		List<Tuple> super_tuples = new ArrayList<Tuple>();
		getSuperTupleList(super_relation, relations, 0, super_tuples, null,
				false);

	}

	public void productOperation() {
		// Creating a single super schema with combined schema of all the tables
		// and combining tuples into one large tuple for where condition

		List<Relation> relations = new ArrayList<Relation>();
		for (String relation_name : tableList) {
			relations.add(dbManager.schema_manager.getRelation(relation_name));
		}

		// Creating super schema
		System.out.print("SELECT: Creating a Super schema" + "\n");
		Schema super_schema = getSuperSchema(relations, false);

		Relation super_relation = dbManager.schema_manager
				.createRelation("SuperRelation", super_schema);
		List<Tuple> super_tuples = new ArrayList<Tuple>();
		getSuperTupleList(super_relation, relations, 0, super_tuples, null,
				true);

	}

	public Schema getSuperSchema(List<Relation> relations,
			boolean isCrossProduct) {
		// Creating super schema, if cross product then append relation name
		// else only adds the field name
		System.out.print("PRODUCT OPERATION: Creating a super schema" + "\n");
		ArrayList<String> field_names = new ArrayList<String>();
		ArrayList<FieldType> field_types = new ArrayList<FieldType>();
		for (Relation relation : relations) {
			Schema schema = relation.getSchema();
			for (int i = 0; i < schema.getFieldTypes().size(); i++) {
				String fieldName = schema.getFieldNames().get(i);
				if (isCrossProduct == false && field_names.contains(fieldName))
					continue;
				if (isCrossProduct)
					fieldName = relation.getRelationName() + '.' + fieldName;
				field_types.add(schema.getFieldTypes().get(i));
				field_names.add(fieldName);
			}
		}
		return new Schema(field_names, field_types);
	}

	/*
	 * Read single block of each relation and create as many super tuples
	 * possible, index is the relation num and mem block index for that relation
	 */
	public void getSuperTupleList(Relation super_relation,
			List<Relation> relations, int index, List<Tuple> superTuples,
			Tuple currSuperTuple, boolean isCrossProduct) {
		if (index == relations.size()) {
			Tuple tuple = super_relation.createTuple();
			copyTupleFields(currSuperTuple, tuple, null, false);
			superTuples.add(tuple);
			return;
		}

		Relation relation = relations.get(index);
		for (int i = 0; i < relation.getNumOfBlocks(); i++) {
			relation.getBlock(i, index);
			ArrayList<Tuple> tuples = dbManager.mem.getBlock(index).getTuples();
			for (Tuple tuple : tuples) {
				copyTupleFields(tuple, currSuperTuple,
						relation.getRelationName(), isCrossProduct);
				getSuperTupleList(super_relation, relations, index + 1,
						superTuples, currSuperTuple, isCrossProduct);
			}
		}
	}

	public void copyTupleFields(Tuple t1, Tuple t2, String relName,
			boolean appendRelName) {
		for (int offset = 0; offset < t1.getNumOfFields(); offset++) {
			String field_name = t1.getSchema().getFieldName(offset);
			if (appendRelName)
				field_name += relName + '.' + field_name;
			if (t1.getField(offset).type == FieldType.INT) {
				t2.setField(field_name, t1.getField(offset).integer);
			} else
				t2.setField(field_name, t1.getField(offset).str);
		}
	}
}
