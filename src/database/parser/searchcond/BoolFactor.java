package database.parser.searchcond;

import database.GlobalVariable;
import database.parser.StmtInterface;

public class BoolFactor {
	boolean isNot;
	BoolPrimary boolPrimary;

	public void create(String query) {
		if (query.contains("NOT")) {
			isNot = true;
			query = query.substring(query.indexOf("NOT") + 4);
		}
		if (GlobalVariable.isTest)
			System.out.println("BOOLFACT--> isNOT:" + isNot + " RAWBOOL PRIMARY:" + query);
		boolPrimary = new BoolPrimary();
		boolPrimary.create(query);
	}

	public boolean execute() {
		boolean result = boolPrimary.execute();
		return isNot?!result:result;
	}

}
