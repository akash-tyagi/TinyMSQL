package database.parser.searchcond;

import database.GlobalVariable;
import database.parser.StmtInterface;

public class BoolFactor implements StmtInterface {
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

	@Override
	public void execute() {
		// TODO Auto-generated method stub
		
	}

}
