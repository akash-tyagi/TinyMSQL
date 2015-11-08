package database.parser.searchcond;

import database.GlobalVariable;
import storageManager.Tuple;

public class BoolFactor {
	boolean isNot;
	BoolPrimary boolPrimary;

	public void create(String query) {
		if (query.contains("NOT")) {
			isNot = true;
			query = query.substring(query.indexOf("NOT") + 4);
		}
		if (GlobalVariable.isTest)
			System.out.println(
					"BOOLFACT--> isNOT:" + isNot + " RAWBOOL PRIMARY:" + query);
		boolPrimary = new BoolPrimary();
		boolPrimary.create(query);
	}

	public boolean execute(Tuple tuple) {
		boolean result = boolPrimary.execute(tuple);
		return isNot ? !result : result;
	}

}
