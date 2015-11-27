package database.logicalquerytree;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import database.DbManager;
import database.parser.SelectStmt;
import database.parser.searchcond.SearchCond;
import storageManager.Relation;

public class LogicalQuery {
	ProjectionOperator projection;
	DbManager dbManager;
	Map<List<Relation>, SearchCond> map = new HashMap<List<Relation>, SearchCond>();

	public LogicalQuery(SelectStmt stmt) {
		System.out.println("IN LOGICAL QUERY");
		List<String> tables = stmt.tableList;
		dbManager = stmt.dbManager;
		SearchCond cond = stmt.cond;

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
				map.put(relations, cond.getSelectionCond(relations));
			}
		}
	}

	// Expecting at max 2 tables for optimization
	public SearchCond getSelectionOptimizationCond(List<String> tables) {
		List<Relation> relations = new ArrayList<Relation>();
		if (tables.size() == 1) {
			relations.add(dbManager.schema_manager.getRelation(tables.get(0)));
			return map.get(relations);
		}
		Relation rel1 = dbManager.schema_manager.getRelation(tables.get(0));
		Relation rel2 = dbManager.schema_manager.getRelation(tables.get(1));
		relations.add(rel1);
		relations.add(rel2);
		SearchCond temp = map.get(relations);
		if (temp != null)
			return temp;

		relations = new ArrayList<Relation>();
		relations.add(rel2);
		relations.add(rel1);
		return map.get(relations);
	}

	public void printSelectionOptimizations() {
		for (List<Relation> relations : map.keySet()) {
			System.out.print("\nFor Relations: \n");
			for (Relation relation : relations) {
				System.out.print(relation.getRelationName() + ",");
			}
			SearchCond temp = map.get(relations);
			if (temp != null)
				temp.print();
			else
				System.out.println("NO Condition");
		}
	}

	public void create(SelectStmt stmt) {
		// projection.execute(stmt);
	}
}