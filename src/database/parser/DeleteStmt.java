package database.parser;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import database.DbManager;
import database.parser.searchcond.SearchCond;
import storageManager.Block;
import storageManager.Relation;
import storageManager.Tuple;

public class DeleteStmt extends StmtBase implements StmtInterface {
	String tableName;
	SearchCond cond;

	public DeleteStmt(DbManager manager) {
		super(manager);
	}

	@Override
	public void create(String query) {
		Pattern pattern = Pattern.compile("DELETE FROM (.*)");
		int type = 0;
		if (query.contains("WHERE")) {
			pattern = Pattern
					.compile("DELETE FROM\\s*([a-z][a-z0-9]*)\\s* WHERE (.*)");
			type = 1;
		}
		Matcher matcher = pattern.matcher(query);
		if (matcher.find()) {
			tableName = matcher.group(1);
			System.out.println("DELETE Statement: TableName:" + tableName);
			if (type == 1) {
				cond = new SearchCond();
				cond.create(matcher.group(2));
			}
		} else {
			System.out.println("ERROR ::: DELETE statement: Invalid:" + query);
			System.exit(1);
		}

	}

	@Override
	public void execute() {
		Relation relation = dbManager.schema_manager.getRelation(tableName);
		int memIndex = 0;
		for (int i = 0; i < relation.getNumOfBlocks(); i++) {
			relation.getBlock(i, memIndex);
			Block block_reference = dbManager.mem.getBlock(memIndex);
			boolean isTupleRemoved = false;
			boolean isBlockEmpty = true;
			for (int j = 0; j < block_reference.getNumTuples(); j++) {
				Tuple tuple = block_reference.getTuple(j);
				// TODO Assumption: deleting all tuples if where cond is null
				if (cond == null || cond.execute(tuple)) {
					block_reference.invalidateTuple(j);
					isTupleRemoved = true;
					continue;
				}
				// Condition will only be set to false if some tuple is still
				// left in block
				isBlockEmpty = false;
			}

			if (isBlockEmpty)
				relation.deleteBlocks(i);
			else if (isTupleRemoved)
				relation.setBlock(i, memIndex);
		}
	}

}
