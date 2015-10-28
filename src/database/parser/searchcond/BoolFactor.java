package database.parser.searchcond;

import database.GlobalVariable;
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
		if (GlobalVariable.isTest)
			System.out.println("BOOLFACT--> isNOT:" + isNot
					+ " RAWBOOL PRIMARY:" + query);
		boolPrimary = new BoolPrimary();
		boolPrimary.create(query);
	}

}
