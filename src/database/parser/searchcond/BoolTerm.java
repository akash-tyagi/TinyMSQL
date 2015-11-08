package database.parser.searchcond;

import java.util.ArrayList;
import java.util.List;

import storageManager.Tuple;

public class BoolTerm {
	BoolTerm boolTerm;
	BoolFactor boolFactor;

	public void create(String query) {
		String rawBoolFactor = query;

		List<Integer> factorList = new ArrayList<Integer>();
		int j = 0;
		for (int i = 0; i < query.length(); i++) {
			char ch = query.charAt(i);
			if (ch == '[')
				j++;
			else if (ch == ']')
				j--;
			factorList.add(j);
		}

		List<Integer> indexList = new ArrayList<Integer>();
		String query2 = "";
		for (int i = 0; i < factorList.size(); i++) {
			if (factorList.get(i) == 0) {
				indexList.add(i);
				query2 += query.charAt(i);
			}
		}
		int index = -1;
		if (query2.contains("AND")) {
			index = indexList.get(query2.indexOf("AND"));
			System.out.println(
					"BOOLTERM-->RAWBOOL TERM:" + query.substring(index + 4));
			boolTerm = new BoolTerm();
			boolTerm.create(query.substring(index + 4));
			rawBoolFactor = query.substring(0, index - 1);
		}

		// if (rawBoolFactor.contains("AND")) {
		// Pattern pattern = Pattern.compile("(.*) AND (.*)");
		// Matcher matcher = pattern.matcher(rawBoolFactor);
		// if (matcher.find()) {
		// rawBoolFactor = matcher.group(1);
		// System.out.println("BOOLTERM-->RAWBOOL TERM:"
		// + matcher.group(2));
		// boolTerm = new BoolTerm();
		// boolTerm.create(matcher.group(2));
		// } else {
		// System.out.println("ERROR ::: BOOLTERM Invalid:" + query);
		// System.exit(1);
		// }
		// }
		System.out.println("BOOLTERM-->RAWBOOL FACTOR:" + rawBoolFactor);
		boolFactor = new BoolFactor();
		boolFactor.create(rawBoolFactor);
	}

	public boolean execute(Tuple tuple) {
		return boolFactor.execute(tuple) && boolTerm.execute(tuple);
	}
}
