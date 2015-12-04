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
		System.out.println(" ######## IN LOGICAL QUERY #######");
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
				System.out.println(
						"\nADDING:" + tables.get(i) + ":" + tables.get(j));
				// cond.getSelectionCond(relations).print();
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