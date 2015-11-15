package database.parser.searchcond;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import database.GlobalVariable;
import storageManager.Relation;
import storageManager.Schema;
import storageManager.Tuple;

public class BoolPrimary {
	SearchCond cond;
	CompPredicate compPred;

	public BoolPrimary() {

	}

	public BoolPrimary(SearchCond cond) {
		this.cond = cond;
	}

	public BoolPrimary(CompPredicate compPredicate) {
		this.compPred = compPredicate;
	}

	public void create(String query) {
		if (query.contains("[")) {
			Pattern pattern = Pattern.compile("\\s*\\[ (.*) \\]\\s*");
			Matcher matcher = pattern.matcher(query);
			if (matcher.find()) {
				cond = new SearchCond();
				cond.create(matcher.group(1));
				return;
			} else {
				System.out.println("ERROR ::: BOOL PRIMARY Invalid:" + query);
				System.exit(1);
			}
		}
		if (GlobalVariable.isTestExecution)
			System.out.println("BoolPrimeary-->RAW COMP PREDICATE:" + query);
		compPred = new CompPredicate();
		compPred.create(query);
	}

	public boolean execute(Tuple tuple) {
		if (compPred == null && cond == null)
			return true;
		return compPred != null ? compPred.execute(tuple) : cond.execute(tuple);
	}

	public BoolPrimary getSelectionCond(List<Relation> relations) {
		if (compPred != null) {
			CompPredicate pred = compPred.getSelectionCond(relations);
			if (pred != null)
				return new BoolPrimary(pred);
			return null;
		}
		SearchCond searchCond = cond.getSelectionCond(relations);
		if (searchCond != null)
			return new BoolPrimary(searchCond);
		return null;
	}

	public void print() {
		if (compPred != null)
			compPred.print();
		else
			cond.print();
	}

}
