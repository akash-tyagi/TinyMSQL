package database.logicalquerytree;

import database.parser.SelectStmt;
import database.parser.searchcond.SearchCond;

public class SelectionOperator {
	SearchCond cond;
	ProductOperator productOperator;

	public SelectionOperator(SearchCond cond) {
		this.cond = cond;
	}

	public void execute(SelectStmt stmt) {
		productOperator = new ProductOperator(stmt.tableList);
	}
}
