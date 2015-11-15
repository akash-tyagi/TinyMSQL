package database.logicalquerytree;

import java.util.ArrayList;
import java.util.List;

import database.DbManager;
import database.parser.SelectStmt;
import database.parser.searchcond.SearchCond;
import storageManager.Relation;

public class LogicalQuery {
	ProjectionOperator projection;

	public LogicalQuery(SelectStmt stmt) {
		// projection = new ProjectionOperator(stmt);
		List<String> tables = stmt.tableList;
		List<Relation> relations = new ArrayList<Relation>();
		DbManager dbManager = stmt.dbManager;
		SearchCond cond = stmt.cond;
		for (String table : tables) {
			relations.add(dbManager.schema_manager.getRelation(table));
			System.out.println("\n\nSelection Condition For Table:" + table);
		}
		SearchCond temp = cond.getSelectionCond(relations);
		if (temp != null)
			temp.print();
		else
			System.out.println("NO CONDITION");
	}

	public void create(SelectStmt stmt) {
		// projection.execute(stmt);
	}
}
