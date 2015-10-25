package database.parser.searchcond;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import database.parser.Stmt;

public class SearchCond implements Stmt {
	BoolTerm boolTerm;
	SearchCond cond;

	@Override
	public void create(String query) {
		String rawBoolTerm = query;
		if (query.contains("OR")) {
			Pattern pattern = Pattern.compile("(.*) OR (.*)");
			Matcher matcher = pattern.matcher(query);
			rawBoolTerm = matcher.group(1);
			cond = new SearchCond();
			cond.create(matcher.group(2));
		}
		boolTerm = new BoolTerm();
		boolTerm.create(rawBoolTerm);
	}

}
