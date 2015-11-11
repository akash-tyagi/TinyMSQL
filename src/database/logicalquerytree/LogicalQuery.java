package database.logicalquerytree;

import database.parser.SelectStmt;

public class LogicalQuery {
	ProjectionOperator projection;

	public LogicalQuery(SelectStmt stmt) {
	}

	public void create(SelectStmt stmt) {
		projection = new ProjectionOperator(stmt.selectList);
		projection.execute(stmt);
	}
}
