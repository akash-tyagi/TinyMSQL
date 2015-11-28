package database.physicalquery;

import database.DbManager;
import database.parser.SelectStmt;
import database.parser.StmtInterface;

public class PhysicalTree {
	OperatorInterface operator;
	DbManager dbManager;

	public PhysicalTree(DbManager dbManager, StmtInterface stmt) {
		this.dbManager = dbManager;
		JoinOptimization jOptimization = new JoinOptimization(dbManager);

		if (stmt instanceof SelectStmt)
			constructSelectTree(stmt);

		// SelectStmt selectStmt = (SelectStmt) stmt;
		// LogicalQuery logicalQuery = new LogicalQuery(selectStmt);
		// String[] joinOrder = jOptimization
		// .getLeftJoinOptimizedSequence(selectStmt.tableList);
		//
		// if (joinOrder.length <= 1)
		// // think about something for this case
		// return;
		//
		// // Constructing Join operator tree
		// operator = new JoinOperator(joinOrder[0], joinOrder[1], manager);
		// JoinOperator tempOperator = (JoinOperator) operator;
		// for (int i = 2; i < joinOrder.length; i++) {
		// JoinOperator joinOperator = new JoinOperator(
		// tempOperator.getResultTableName(), joinOrder[i],
		// manager);
		// tempOperator.setNextOperator(joinOperator);
		// tempOperator = joinOperator;
		// }
	}

	private void constructSelectTree(StmtInterface stmt) {
		SelectStmt selectStmt = (SelectStmt) stmt;
		// SINGLE TABLE SELECT OPERATOR
		if (selectStmt.tableList.size() == 1) {
			operator = new SelectOperator(dbManager,
					selectStmt.tableList.get(0), selectStmt.cond);
			OperatorInterface nextOperator, prevOperator;
			prevOperator = operator;
			if (selectStmt.isDistinct) {
				// add a duplicate removal operator
			}
			if (selectStmt.selectList != null
					&& !selectStmt.selectList.get(0).equals("*")) {
				System.out.println(selectStmt.selectList);
				System.out.println("ADDING PROJECTION");
				nextOperator = new ProjectionOperator(dbManager,
						selectStmt.selectList);
				prevOperator.setNextOperator(nextOperator);
			}
		}
	}

	public void execute() {
		if (operator != null)
			operator.execute();
	}
}
