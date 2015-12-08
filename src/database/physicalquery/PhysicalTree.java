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
	String query;
	JoinOptimization joinOptimization;

	public PhysicalTree(String query, DbManager dbManager, StmtInterface stmt,
			PrintWriter writer) {
		this.dbManager = dbManager;
		this.writer = writer;
		this.query = query;
		joinOptimization = new JoinOptimization(dbManager);
		if (stmt instanceof SelectStmt) {
			logicalQuery = new LogicalQuery((SelectStmt) stmt);
			constructSelectTree(stmt);
		}
	}

	private void constructSelectTree(StmtInterface stmt) {
		boolean isJoin = false;
		SelectStmt selectStmt = (SelectStmt) stmt;
		OperatorInterface nextOperator, currOperator;
		// SINGLE TABLE SELECT OPERATOR
		if (selectStmt.tableList.size() == 1) {
			currOperator = operator = new SelectOperator(dbManager,
					selectStmt.tableList.get(0), selectStmt.cond, writer);
		} // PRODUCT/THETA OPEARATION ONLY FOR NOW
		else {
			if (joinOptimization.generateJoinColumns(query,
					selectStmt.tableList)
					&& joinOptimization.join_columns_map.size() > 0) {
				currOperator = constructJoinTree(selectStmt.tableList);
				isJoin = true;
			} else
				currOperator = constructProductTree(selectStmt.tableList);
		}

		if (selectStmt.isDistinct) {
			List<String> select_list = new ArrayList<String>();
			if (!selectStmt.selectList.get(0).equals("*"))
				select_list = selectStmt.selectList;

			nextOperator = new DuplicateOperator(dbManager, writer,
					selectStmt.orderBy, select_list);
			currOperator.setNextOperator(nextOperator);
			currOperator = nextOperator;
		} else if (selectStmt.orderBy != null) {
			nextOperator = new SortingOperator(dbManager, selectStmt.orderBy,
					writer);
			currOperator.setNextOperator(nextOperator);
			currOperator = nextOperator;
		}
		if (selectStmt.selectList.get(0).equals("*"))
			selectStmt.selectList = null;
		nextOperator = new ProjectionOperator(dbManager, selectStmt.selectList,
				writer, isJoin, joinOptimization.getJoinColumns());
		currOperator.setNextOperator(nextOperator);
		currOperator = nextOperator;
	}

	private OperatorInterface constructProductTree(List<String> join_tables) {
		OperatorInterface head = null, currOperator = null, nextOperator = null;
		List<Integer> tableSizes = new ArrayList<Integer>();
		for (String table : join_tables) {
			int blocks = dbManager.schema_manager.getRelation(table)
					.getNumOfTuples();
			tableSizes.add(blocks);
		}
		int size = join_tables.size();
		List<String> tables = new ArrayList<String>();
		for (int i = 0; i < size; i++) {
			int min = Collections.min(tableSizes);
			int index = tableSizes.indexOf((Integer) min);
			tables.add(join_tables.remove(index));
			tableSizes.remove((Integer) min);
		}

		while (tables.size() > 1) {
			String rel1 = tables.get(0);
			String rel2 = tables.get(1);
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
		return nextOperator;
	}

	private OperatorInterface constructJoinTree(List<String> tables) {
		OperatorInterface head = null, currOperator = null, nextOperator = null;
		String[] join_order = joinOptimization
				.getLeftJoinOptimizedSequence(tables);
		tables = new ArrayList<>();
		for (String string : join_order) {
			tables.add(string);
		}
		List<String> joinColumns = null;

		while (tables.size() > 1) {
			String rel1 = tables.get(0);
			String rel2 = tables.get(1);
			joinColumns = joinOptimization.getJoinColumns(rel1, rel2);
			nextOperator = new JoinOperator(rel1, rel2, dbManager, writer,
					(ArrayList<String>) joinColumns, logicalQuery);
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
		return nextOperator;
	}

	public void execute() {
		if (operator != null)
			System.out.println("Total Tuples:" + operator.execute(true).size());
	}
}
