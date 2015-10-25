package database.parser.searchcond;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import database.parser.Stmt;

public class BoolTerm implements Stmt {
	BoolTerm boolTerm;
	BoolFactor boolFactor;

	@Override
	public void create(String query) {
		String rawBoolPrimary = query;
		if (rawBoolPrimary.contains("AND")) {
			Pattern pattern = Pattern.compile("(.*) AND (.*)");
			Matcher matcher = pattern.matcher(rawBoolPrimary);
			rawBoolPrimary = matcher.group(1);
			boolTerm = new BoolTerm();
			boolTerm.create(matcher.group(2));
		}
		boolFactor = new BoolFactor();
		boolFactor.create(rawBoolPrimary);
	}

}
