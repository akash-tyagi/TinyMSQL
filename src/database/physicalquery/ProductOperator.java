package database.physicalquery;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import database.DbManager;
import database.GlobalVariable;
import database.logicaloptimization.LogicalQuery;
import database.parser.searchcond.SearchCond;
import storageManager.Block;
import storageManager.FieldType;
import storageManager.Relation;
import storageManager.Schema;
import storageManager.Tuple;

public class ProductOperator extends OperatorBase implements OperatorInterface {
	String relation_name2;
	LogicalQuery logicalQuery;
	int block_for_reading_1 = 9;
	int block_for_reading_2 = 8;

	int block_for_writing = 0;

	public ProductOperator(DbManager manager, LogicalQuery logicalQuery,
			String rel1, String rel2, PrintWriter writer) {
		super(manager, writer);
		this.logicalQuery = logicalQuery;
		relation_name = rel1;
		relation_name2 = rel2;
	}

	@Override
	public List<Tuple> execute(boolean printResult) {
		Relation rel1 = dbManager.schema_manager.getRelation(relation_name);
		Relation rel2 = dbManager.schema_manager.getRelation(relation_name2);
		// Table already present in memory
		if (isReadFromMem) {
			System.out.println("READ FROM MEM PRODUCT");
			String rel = readFromMemProductOperation(rel1, rel2, printResult);
			if (next_operator != null)
				next_operator.setRelationName(rel);
		} else {
			// CAN PRODUCT RESULT BE STORED IN MEMORY
			if (isProductMemStorable(rel1, rel2)) {
				System.out.println("MEMORY STORABLE PRODUCT");
				int endBlock = memStorableProductOperation(rel1, rel2,
						printResult);
				if (next_operator != null)
					next_operator.setBlocksNumbers(block_for_writing, endBlock);
			} // CAN ONE PASS JOIN BE DONE
			else if (rel1.getNumOfBlocks() <= GlobalVariable.USABLE_JOIN_BLOCKS
					|| rel2.getNumOfBlocks() <= GlobalVariable.USABLE_JOIN_BLOCKS) {
				System.out.println("ONE PASS PRODUCT");
				String rel = null;
				start_block = 0;
				if (rel1.getNumOfBlocks() <= GlobalVariable.USABLE_JOIN_BLOCKS) {
					end_block = rel1.getNumOfBlocks() - 1;
					for (int i = start_block; i <= end_block; i++) {
						rel1.getBlock(i, i);
					}
					rel = readFromMemProductOperation(rel1, rel2, printResult);
				} else {
					end_block = rel2.getNumOfBlocks() - 1;
					for (int i = start_block; i <= end_block; i++) {
						rel2.getBlock(i, i);
					}
					rel = readFromMemProductOperation(rel2, rel1, printResult);
				}

				if (next_operator != null)
					next_operator.setRelationName(rel);
			} else {
				System.out.println("ACTUAL PRODUCT");
				String rel = productOperation(rel1, rel2, printResult);
				if (next_operator != null)
					next_operator.setRelationName(rel);
			}
		}
		if (next_operator != null)
			return next_operator.execute(printResult);
		return res_tuples;
	}

	private String readFromMemProductOperation(Relation rel1, Relation rel2,
			boolean printResult) {
		SearchCond cond1 = logicalQuery
				.getSelectOptCondSingleTable(rel1.getRelationName());
		SearchCond cond2 = logicalQuery
				.getSelectOptCondSingleTable(rel2.getRelationName());
		List<SearchCond> conds = logicalQuery.getSelectOptConds(
				rel1.getRelationName(), rel2.getRelationName());

		String join_relation_name = rel1.getRelationName() + "_"
				+ rel2.getRelationName();
		dbManager.temporaryCreatedRelations.add(join_relation_name);

		List<Relation> relations = new ArrayList<Relation>();
		relations.add(rel1);
		relations.add(rel2);

		Relation join_relation = getJoinedRelSchema(join_relation_name,
				relations);
		int total_tuples = 0;
		Tuple join_tuple = null;
		block_for_writing = block_for_reading_1;
		Block write_block_ref = dbManager.mem.getBlock(block_for_writing);
		write_block_ref.clear();

		for (int j = 0; j < rel2.getNumOfBlocks(); j++) {
			rel2.getBlock(j, block_for_reading_2);
			Block rel2_block = dbManager.mem.getBlock(block_for_reading_2);
			List<Tuple> rel2_tuples = rel2_block.getTuples();
			for (Tuple tuple2 : rel2_tuples) {
				if (cond2 != null && cond2.execute(tuple2) == false)
					continue;
				for (int i = start_block; i <= end_block; i++) {
					Block rel1_block = dbManager.mem.getBlock(i);
					List<Tuple> rel1_tuples = rel1_block.getTuples();
					for (Tuple tuple1 : rel1_tuples) {
						if (cond1 != null && cond1.execute(tuple1) == false)
							continue;
						join_tuple = join_relation.createTuple();
						List<Tuple> from = new ArrayList<Tuple>();
						from.add(tuple1);
						from.add(tuple2);
						copyTupleFields(from, join_tuple);
						if (conds != null && check_search_conds(conds,
								join_tuple) == false)
							continue;
						total_tuples++;
						if (next_operator == null) {
							if (printResult) {
								System.out.println(join_tuple.toString());
								writer.println(join_tuple.toString());
							}
							res_tuples.add(join_tuple);
							continue;
						}
						while (write_block_ref.isFull() == true
								|| write_block_ref
										.appendTuple(join_tuple) == false) {
							join_relation.setBlock(
									join_relation.getNumOfBlocks(),
									block_for_writing);
							write_block_ref.clear();
						}
					}
				}
			}
			rel2_block.clear();
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

	private Relation getJoinedRelSchema(String join_relation_name,
			List<Relation> relations) {
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
		return join_relation;
	}

	private int memStorableProductOperation(Relation rel1, Relation rel2,
			boolean printResult) {
		SearchCond cond1 = logicalQuery
				.getSelectOptCondSingleTable(rel1.getRelationName());
		SearchCond cond2 = logicalQuery
				.getSelectOptCondSingleTable(rel2.getRelationName());
		List<SearchCond> conds = logicalQuery.getSelectOptConds(
				rel1.getRelationName(), rel2.getRelationName());

		String join_relation_name = rel1.getRelationName() + "_"
				+ rel2.getRelationName();
		List<Relation> relations = new ArrayList<Relation>();
		relations.add(rel1);
		relations.add(rel2);

		Relation join_relation = getJoinedRelSchema(join_relation_name,
				relations);
		dbManager.temporaryCreatedRelations.add(join_relation_name);
		int total_tuples = 0;
		int lastWriteBlock = 0;
		Tuple join_tuple = null;
		Block write_block_ref = dbManager.mem.getBlock(lastWriteBlock);
		write_block_ref.clear();

		for (int i = 0; i < rel1.getNumOfBlocks(); i++) {
			rel1.getBlock(i, block_for_reading_1);
			Block rel1_block = dbManager.mem.getBlock(block_for_reading_1);
			List<Tuple> rel1_tuples = rel1_block.getTuples();

			for (int j = 0; j < rel2.getNumOfBlocks(); j++) {
				rel2.getBlock(j, block_for_reading_2);
				Block rel2_block = dbManager.mem.getBlock(block_for_reading_2);
				List<Tuple> rel2_tuples = rel2_block.getTuples();

				for (Tuple tuple2 : rel2_tuples) {
					if (cond2 != null && cond2.execute(tuple2) == false)
						continue;
					for (Tuple tuple1 : rel1_tuples) {
						if (cond1 != null && cond1.execute(tuple1) == false)
							continue;
						join_tuple = join_relation.createTuple();
						List<Tuple> from = new ArrayList<Tuple>();
						from.add(tuple1);
						from.add(tuple2);
						copyTupleFields(from, join_tuple);
						if (conds != null && check_search_conds(conds,
								join_tuple) == false)
							continue;
						total_tuples++;
						res_tuples.add(join_tuple);
						if (next_operator == null) {
							if (printResult) {
								System.out.println(join_tuple.toString());
								writer.println(join_tuple.toString());
							}
						} else if (next_operator instanceof ProjectionOperator) {
							((ProjectionOperator) next_operator)
									.printTuple(join_tuple, printResult);
						} else {
							// STORE IN MEMORY FOR NEXT OPERATOR
							while (write_block_ref.isFull() == true
									|| write_block_ref
											.appendTuple(join_tuple) == false) {
								lastWriteBlock++;
								write_block_ref = dbManager.mem
										.getBlock(lastWriteBlock);
								write_block_ref.clear();
							}
						}
					}
				}
			}
		}
		System.out.println("PRODUCT:" + relation_name + " and " + relation_name2
				+ " tuples generated:" + total_tuples);
		return lastWriteBlock;
	}

	private boolean isProductMemStorable(Relation rel1, Relation rel2) {
		int total_tuples = rel1.getNumOfTuples() * rel2.getNumOfTuples();
		int field_size = rel1.getSchema().getNumOfFields()
				+ rel2.getSchema().getNumOfFields();
		int blocks_needed = 15;
		if (field_size > 4) {
			blocks_needed = total_tuples;
		} else {
			int i = 1;
			while (field_size * (i + 1) <= 8) {
				i++;
			}
			blocks_needed = total_tuples / i;
		}
		System.out.println("Total Blocks needed:" + blocks_needed);
		if (blocks_needed <= GlobalVariable.USABLE_JOIN_BLOCKS)
			return true;
		return false;
	}

	public String productOperation(Relation rel1, Relation rel2,
			boolean printResult) {
		SearchCond cond1 = logicalQuery
				.getSelectOptCondSingleTable(rel1.getRelationName());
		SearchCond cond2 = logicalQuery
				.getSelectOptCondSingleTable(rel2.getRelationName());
		List<SearchCond> conds = logicalQuery.getSelectOptConds(
				rel1.getRelationName(), rel2.getRelationName());
		
		String join_relation_name = rel1.getRelationName() + "_"
				+ rel2.getRelationName();
		dbManager.temporaryCreatedRelations.add(join_relation_name);
		List<Relation> relations = new ArrayList<Relation>();
		relations.add(rel1);
		relations.add(rel2);
		Relation join_relation = getJoinedRelSchema(join_relation_name,
				relations);
		int total_tuples = 0;
		Tuple join_tuple;
		Block write_block_ref = dbManager.mem.getBlock(block_for_writing);
		write_block_ref.clear();

		for (int i = 0; i < rel1.getNumOfBlocks(); i++) {
			rel1.getBlock(i, block_for_reading_1);
			Block rel1_block = dbManager.mem.getBlock(block_for_reading_1);
			List<Tuple> rel1_tuples = rel1_block.getTuples();
			for (int j = 0; j < rel2.getNumOfBlocks(); j++) {
				rel2.getBlock(j, block_for_reading_2);
				Block rel2_block = dbManager.mem.getBlock(block_for_reading_2);
				List<Tuple> rel2_tuples = rel2_block.getTuples();
				for (Tuple tuple2 : rel2_tuples) {
					if (cond2 != null && cond2.execute(tuple2) == false)
						continue;
					for (Tuple tuple1 : rel1_tuples) {
						if (cond1 != null && cond1.execute(tuple1) == false)
							continue;
						join_tuple = join_relation.createTuple();
						List<Tuple> from = new ArrayList<Tuple>();
						from.add(tuple1);
						from.add(tuple2);
						copyTupleFields(from, join_tuple);
						if (conds != null && check_search_conds(conds,
								join_tuple) == false)
							continue;
						total_tuples++;
						res_tuples.add(join_tuple);
						if (next_operator == null) {
							if (printResult) {
								System.out.println(join_tuple.toString());
								writer.println(join_tuple.toString());
							}
							continue;
						} else if (next_operator instanceof ProjectionOperator) {
							((ProjectionOperator) next_operator)
									.printTuple(join_tuple, printResult);
						} else {
							while (write_block_ref.isFull() == true
									|| write_block_ref
											.appendTuple(join_tuple) == false) {
								join_relation.setBlock(
										join_relation.getNumOfBlocks(),
										block_for_writing);
								write_block_ref.clear();
							}
						}
					}
				}
			}
		}
		// WRITE LAST BLOCK PENDING IN MEMORY If NOT EMPTY
		if (!(next_operator instanceof ProjectionOperator)) {
			if (next_operator != null && !write_block_ref.isEmpty()) {
				join_relation.setBlock(join_relation.getNumOfBlocks(),
						block_for_writing);
				write_block_ref.clear();
			}
		} else {
			next_operator = null;
		}
		System.out.println("PRODUCT:" + relation_name + " and " + relation_name2
				+ " tuples generated:" + total_tuples);
		return join_relation_name;
	}

	private boolean check_search_conds(List<SearchCond> conds, Tuple tuple) {
		for (SearchCond searchCond : conds) {
			if (!searchCond.execute(tuple))
				return false;
		}
		return true;
	}

	public void copyTupleFields(List<Tuple> from, Tuple to) {
		int i = 0;
		for (Tuple tuple : from) {
			for (int offset = 0; offset < tuple.getNumOfFields(); offset++) {
				if (tuple.getField(offset).type == FieldType.INT) {
					to.setField(i, tuple.getField(offset).integer);
				} else {
					to.setField(i, tuple.getField(offset).str);
				}
				i++;
			}
		}
	}
}
