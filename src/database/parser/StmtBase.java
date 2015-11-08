package database.parser;

import database.DbManager;

public class StmtBase {
	DbManager dbManager;

	public StmtBase(DbManager dbManager) {
		this.dbManager = dbManager;
	}
}
