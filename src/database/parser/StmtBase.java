package database.parser;

import database.Manager;

public class StmtBase {
	Manager manager;

	public StmtBase(Manager manager) {
		this.manager = manager;
	}
}
