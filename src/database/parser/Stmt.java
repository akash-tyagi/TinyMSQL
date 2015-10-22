package database.parser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class Stmt {
	Pattern pattern;
	Matcher matcher;

	public abstract void create(String query);
}
