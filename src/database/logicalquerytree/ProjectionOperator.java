package database.logicalquerytree;

import java.util.List;

import database.parser.SelectStmt;

public class ProjectionOperator {
	List<String> projectList;
	SelectionOperator selectionOperator;

	public ProjectionOperator(SelectStmt stmt) {
		this.projectList = stmt.selectList;
		selectionOperator = new SelectionOperator(stmt);
	}

	public void execute(SelectStmt stmt) {
		// selectionOperator.execute(stmt);
	}
}
