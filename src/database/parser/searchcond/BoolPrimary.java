package database.parser.searchcond;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import database.GlobalVariable;
import storageManager.Tuple;

public class BoolPrimary {
	SearchCond cond;
	CompPredicate compPred;

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
		return compPred != null ? compPred.execute(tuple) : cond.execute(tuple);
	}

}
