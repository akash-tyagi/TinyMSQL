package database.logicalquerytree;

import database.parser.SelectStmt;
import database.parser.searchcond.SearchCond;

public class SelectionOperator {
	SearchCond cond;
	ProductOperator productOperator;

	public SelectionOperator(SelectStmt stmt) {
		this.cond = stmt.cond;
		productOperator = new ProductOperator(stmt);
	}

	public void execute(SelectStmt stmt) {
		
	}
}
