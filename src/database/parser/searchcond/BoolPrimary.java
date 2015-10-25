package database.parser.searchcond;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import database.parser.Stmt;

public class BoolPrimary implements Stmt {
	SearchCond cond;
	CompPredicate compPred;

	@Override
	public void create(String query) {
		if (query.contains("[")) {
			Pattern pattern = Pattern.compile("\\s*[(*)]\\s*");
			Matcher matcher = pattern.matcher(query);
			cond = new SearchCond();
			cond.create(matcher.group(1));
			return;
		}
		compPred = new CompPredicate();
		compPred.create(query);
	}

}
