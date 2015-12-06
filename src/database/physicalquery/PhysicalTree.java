package database.physicalquery;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import database.DbManager;
import database.logicaloptimization.LogicalQuery;
import database.parser.SelectStmt;
import database.parser.StmtInterface;

public class PhysicalTree {
	public OperatorInterface operator;
	DbManager dbManager;
	LogicalQuery logicalQuery;
	PrintWriter writer;

	public PhysicalTree(DbManager dbManager, StmtInterface stmt,
			PrintWriter writer) {
		this.dbManager = dbManager;
		this.writer = writer;
		JoinOptimization jOptimization = new JoinOptimization(dbManager);

		if (stmt instanceof SelectStmt) {
			logicalQuery = new LogicalQuery((SelectStmt) stmt);
			// logicalQuery.printSelectionOptimizations();
			constructSelectTree(stmt);
		}
	}

	private void constructSelectTree(StmtInterface stmt) {
		SelectStmt selectStmt = (SelectStmt) stmt;
		OperatorInterface nextOperator, currOperator;
		// SINGLE TABLE SELECT OPERATOR
		if (selectStmt.tableList.size() == 1) {
			currOperator = operator = new SelectOperator(dbManager,
					selectStmt.tableList.get(0), selectStmt.cond, writer);
		} // PRODUCT/THETA OPEARATION ONLY FOR NOW
		else {
			currOperator = constructProductTree(selectStmt.tableList);
		}

		if (selectStmt.isDistinct) {
			// add a duplicate removal operator
		}
		if (selectStmt.orderBy != null) {
			nextOperator = new SortingOperator(dbManager, selectStmt.orderBy,
					writer);
			currOperator.setNextOperator(nextOperator);
			currOperator = nextOperator;
		}
		if (selectStmt.selectList != null
				&& !selectStmt.selectList.get(0).equals("*")) {
			System.out.println(selectStmt.selectList);
			System.out.println("ADDING PROJECTION");
			nextOperator = new ProjectionOperator(dbManager,
					selectStmt.selectList, writer);
			currOperator.setNextOperator(nextOperator);
			currOperator = nextOperator;
		}
	}

	private OperatorInterface constructProductTree(List<String> join_tables) {
		OperatorInterface head = null, currOperator = null, nextOperator = null;
		List<Integer> tableSizes = new ArrayList<Integer>();
		for (String table : join_tables) {
			int blocks = dbManager.schema_manager.getRelation(table)
					.getNumOfBlocks();
			tableSizes.add(blocks);
		}
		int size = join_tables.size();
		List<String> tables = new ArrayList<String>();

		for (int i = 0; i < size; i++) {
			int min = Collections.min(tableSizes);
			int index = tableSizes.indexOf(min);
			tables.add(join_tables.remove(index));
		}

		while (tables.size() > 1) {
			String rel1 = tables.get(0);
			String rel2 = tables.get(1);
			System.out.println("Combining tables:" + rel1 + ":" + rel2);
			nextOperator = new ProductOperator(dbManager, logicalQuery, rel1,
					rel2, writer);
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
			operator.execute(true);
	}
}
