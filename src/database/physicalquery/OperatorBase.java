package database.physicalquery;

import database.DbManager;

public class OperatorBase {
	DbManager dbManager;
	OperatorInterface nextOperator;
	boolean isReadFromMem;
	String relation_name;

	int START_BLOCK;
	int FINAL_BLOCK;

	public OperatorBase(DbManager manager) {
		this.dbManager = manager;
	}

	public void setNextOperator(OperatorInterface operator) {
		this.nextOperator = operator;
	}

	public void setBlocksNumbers(int start, int end) {
		START_BLOCK = start;
		FINAL_BLOCK = end;
		isReadFromMem = true;
	}

	public void setRelationName(String realation_name) {
		this.relation_name = realation_name;
	}
}
