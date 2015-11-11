package database.logicalquerytree;

import java.util.List;

import database.parser.SelectStmt;

public class ProjectionOperator {
	List<String> projectList;
	SelectionOperator selectionOperator;

	public ProjectionOperator(List<String> projectList) {
		this.projectList = projectList;
	}

	public void execute(SelectStmt stmt) {
		selectionOperator = new SelectionOperator(stmt.cond);
		selectionOperator.execute(stmt);
	}
}
