package database.physicalquery;

import database.DbManager;

public class OperatorBase {
	DbManager dbManager;
	OperatorInterface operator;

	public OperatorBase(DbManager manager) {
		this.dbManager = manager;
	}
	
	public void setNextOperator(OperatorInterface operator) {
		this.operator = operator;
	}
}
