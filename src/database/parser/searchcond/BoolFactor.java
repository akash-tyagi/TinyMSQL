package database.parser.searchcond;

import database.parser.Stmt;

public class BoolFactor implements Stmt {
	boolean isNot;
	BoolPrimary boolPrimary;

	@Override
	public void create(String query) {
		if (query.contains("NOT")) {
			isNot = true;
			query = query.substring(query.indexOf("NOT") + 4);
		}
		boolPrimary.create(query);
	}

}
