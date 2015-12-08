package database.logicaloptimization;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import database.DbManager;
import database.parser.SelectStmt;
import database.parser.searchcond.SearchCond;
import storageManager.Relation;

public class LogicalQuery {
	DbManager dbManager;
	Map<List<Relation>, SearchCond> map = new HashMap<List<Relation>, SearchCond>();

	public LogicalQuery(SelectStmt stmt) {
		List<String> tables = stmt.tableList;
		dbManager = stmt.dbManager;
		SearchCond cond = stmt.cond;
		if (cond == null)
			return;
		for (String table : tables) {
			List<Relation> relations = new ArrayList<Relation>();
			relations.add(dbManager.schema_manager.getRelation(table));
			map.put(relations, cond.getSelectionCond(relations));
		}

		for (int i = 0; i < tables.size() - 1; i++) {
			for (int j = i + 1; j < tables.size(); j++) {
				List<Relation> relations = new ArrayList<Relation>();
				relations.add(
						dbManager.schema_manager.getRelation(tables.get(i)));
				relations.add(
						dbManager.schema_manager.getRelation(tables.get(j)));
				System.out.println(
						"\nADDING:" + tables.get(i) + ":" + tables.get(j));
				cond.getSelectionCond(relations).print();
				map.put(relations, cond.getSelectionCond(relations));
			}
		}
	}

	// Expecting at max 2 tables for optimization
	public List<SearchCond> getSelectOptConds(String table1, String table2) {
		String[] tables = table1.split("\\_");
		List<SearchCond> conds = new ArrayList<>();
		for (String table : tables) {
			SearchCond temp = getSelectOptCondMulTable(table, table2);
			if (temp != null)
				conds.add(temp);
		}
		return conds.size() > 0 ? conds : null;
	}
	// if (table1.contains("_"))
	// return getOptimization(table1, table2);
	// List<SearchCond> conds = new ArrayList<>();
	// List<Relation> relations = new ArrayList<>();
	//
	// Relation rel1 = dbManager.schema_manager.getRelation(table1);
	// Relation rel2 = dbManager.schema_manager.getRelation(table2);
	// relations.add(rel1);
	// relations.add(rel2);
	// SearchCond temp = map.get(relations);
	// if (temp != null) {
	// conds.add(temp);
	// return conds;
	// }
	//
	// relations = new ArrayList<Relation>();
	// relations.add(rel2);
	// relations.add(rel1);
	// temp = map.get(relations);
	// if (temp != null) {
	// conds.add(temp);
	// return conds;
	// }
	// return null;
	// }

	// private List<SearchCond> getOptimization(String table1, String table2) {
	// String[] tables = table1.split("\\_");
	// List<SearchCond> conds = new ArrayList<>();
	// for (String table : tables) {
	// SearchCond temp = getSelectOptConds(table, table2);
	// }
	// return null;
	// }

	private SearchCond getSelectOptCondMulTable(String table1, String table2) {
		List<Relation> relations = new ArrayList<>();

		Relation rel1 = dbManager.schema_manager.getRelation(table1);
		Relation rel2 = dbManager.schema_manager.getRelation(table2);
		relations.add(rel1);
		relations.add(rel2);
		SearchCond temp = map.get(relations);
		if (temp != null) {
			return temp;
		}

		relations = new ArrayList<Relation>();
		relations.add(rel2);
		relations.add(rel1);
		temp = map.get(relations);
		return temp;
	}

	public SearchCond getSelectOptCondSingleTable(String table) {
		List<Relation> relations = new ArrayList<Relation>();
		relations.add(dbManager.schema_manager.getRelation(table));
		return map.get(relations);
	}

	public void printSelectionOptimizations() {
		System.out.println("\n########SELECTION OPTIMIZATION #####");
		for (List<Relation> relations : map.keySet()) {
			for (Relation relation : relations) {
				System.out.print(relation.getRelationName() + ",");
			}
			System.out.print("::");
			SearchCond temp = map.get(relations);
			if (temp != null) {
				temp.print();
				System.out.println("");
			} else
				System.out.println("NO Condition");
		}
		System.out.println("");
	}

	public void create(SelectStmt stmt) {
		// projection.execute(stmt);
	}
}