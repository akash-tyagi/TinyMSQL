package database.physicalquery;

import database.DbManager;

public class OperatorBase {
	DbManager dbManager;
	OperatorInterface next_operator;
	boolean isReadFromMem;
	String relation_name;

	int start_block;
	int end_block;

	public OperatorBase(DbManager dbManager) {
		this.dbManager = dbManager;
	}

	public void setNextOperator(OperatorInterface operator) {
		this.next_operator = operator;
	}

	public void setBlocksNumbers(int start, int end) {
		start_block = start;
		end_block = end;
		isReadFromMem = true;
	}

	public void setRelationName(String realation_name) {
		this.relation_name = realation_name;
	}
}
