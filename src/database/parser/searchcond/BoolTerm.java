package database.parser.searchcond;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import database.GlobalVariable;
import database.parser.Stmt;

public class BoolTerm implements Stmt {
	BoolTerm boolTerm;
	BoolFactor boolFactor;

	@Override
	public void create(String query) {
		String rawBoolFactor = query;
		if (rawBoolFactor.contains("AND")) {
			Pattern pattern = Pattern.compile("\\s*(.*) AND (.*)\\s*");
			Matcher matcher = pattern.matcher(rawBoolFactor);
			if (matcher.find()) {
				rawBoolFactor = matcher.group(1);
				boolTerm = new BoolTerm();
				boolTerm.create(matcher.group(2));
			} else {
				System.out.println("ERROR ::: BOOLTERM Invalid:" + query);
				System.exit(1);
			}
		}
		if (GlobalVariable.isTest)
			System.out.println("BOOLTERM-->RAWBOOL FACTOR:" + rawBoolFactor);
		boolFactor = new BoolFactor();
		boolFactor.create(rawBoolFactor);
	}
}
