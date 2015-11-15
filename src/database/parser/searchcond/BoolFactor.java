package database.parser.searchcond;

import java.util.List;

import database.GlobalVariable;
import storageManager.Relation;
import storageManager.Schema;
import storageManager.Tuple;

public class BoolFactor {
	boolean isNot;
	BoolPrimary boolPrimary;

	public BoolFactor() {
	}

	public BoolFactor(BoolPrimary boolPrimary, boolean isNot) {
		this.boolPrimary = boolPrimary;
		this.isNot = isNot;
	}

	public void create(String query) {
		if (query.contains("NOT")) {
			isNot = true;
			query = query.substring(query.indexOf("NOT") + 4);
		}
		if (GlobalVariable.isTestExecution)
			System.out.println(
					"BOOLFACT--> isNOT:" + isNot + " RAWBOOL PRIMARY:" + query);
		boolPrimary = new BoolPrimary();
		boolPrimary.create(query);
	}

	public boolean execute(Tuple tuple) {
		boolean result = boolPrimary.execute(tuple);
		return isNot ? !result : result;
	}

	public BoolFactor getSelectionCond(List<Relation> relations) {
		BoolPrimary bPrimary = boolPrimary.getSelectionCond(relations);
		if (bPrimary != null)
			return new BoolFactor(boolPrimary.getSelectionCond(relations),
					isNot);
		return null;
	}

	public void print() {
		if (isNot)
			System.out.print(" ! ");
		boolPrimary.print();
	}

}
