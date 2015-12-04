package database.physicalquery;

import java.util.List;
import database.DbManager;
import database.logicaloptimization.LogicalQuery;
import database.parser.SelectStmt;
import database.parser.StmtInterface;

public class PhysicalTree {
	OperatorInterface operator;
	DbManager dbManager;

	public PhysicalTree(DbManager dbManager, StmtInterface stmt) {
		this.dbManager = dbManager;
		JoinOptimization jOptimization = new JoinOptimization(dbManager);

		if (stmt instanceof SelectStmt) {
			LogicalQuery lQuery = new LogicalQuery((SelectStmt) stmt);
			lQuery.printSelectionOptimizations();
			// constructSelectTree(stmt);
		}
	}

	private void constructSelectTree(StmtInterface stmt) {
		SelectStmt selectStmt = (SelectStmt) stmt;
		OperatorInterface nextOperator, currOperator;
		// SINGLE TABLE SELECT OPERATOR
		if (selectStmt.tableList.size() == 1) {
			currOperator = operator = new SelectOperator(dbManager,
					selectStmt.tableList.get(0), selectStmt.cond);
		} // PRODUCT/THETA OPEARATION ONLY FOR NOW
		else {
			currOperator = constructProductTree(selectStmt.tableList);
		}

		if (selectStmt.isDistinct) {
			// add a duplicate removal operator
		}
		if (selectStmt.orderBy != null) {
			nextOperator = new SortingOperator(dbManager, selectStmt.orderBy);
			currOperator.setNextOperator(nextOperator);
			currOperator = nextOperator;
		}
		if (selectStmt.selectList != null
				&& !selectStmt.selectList.get(0).equals("*")) {
			System.out.println(selectStmt.selectList);
			System.out.println("ADDING PROJECTION");
			nextOperator = new ProjectionOperator(dbManager,
					selectStmt.selectList);
			currOperator.setNextOperator(nextOperator);
			currOperator = nextOperator;
		}
	}

	private OperatorInterface constructProductTree(List<String> tables) {
		OperatorInterface head = null, currOperator = null, nextOperator = null;
		while (tables.size() > 1) {
			String rel1 = tables.get(0);
			String rel2 = tables.get(1);
			System.out.println("Combining tables:" + rel1 + ":" + rel2);
			nextOperator = new ProductOperator(dbManager, rel1, rel2);
			String newRel = rel1 + "_" + rel2;
			tables.remove(0);
			tables.remove(0);
			tables.add(0, newRel);
			if (head == null)
				currOperator = operator = head = nextOperator;
			else {
				currOperator.setNextOperator(nextOperator);
				currOperator = nextOperator;
			}
		}
		System.out.println("Final Table:" + tables.get(0));
		return nextOperator;
	}

	public void execute() {
		if (operator != null)
			operator.execute();
	}
}
