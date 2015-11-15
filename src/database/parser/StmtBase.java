package database.parser;

import database.DbManager;

public class StmtBase {
	public DbManager dbManager;

	public StmtBase(DbManager dbManager) {
		this.dbManager = dbManager;
	}
}
