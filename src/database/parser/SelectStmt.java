package database.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import database.DbManager;
import database.parser.searchcond.SearchCond;
import storageManager.Relation;
import storageManager.Schema;
import storageManager.Tuple;

public class SelectStmt extends StmtBase implements StmtInterface {

    boolean isDistinct = false;
    List<String> selectList;
    List<String> tableList;
    SearchCond cond;
    String orderBy;

    public SelectStmt(DbManager manager) {
        super(manager);
        selectList = new ArrayList<String>();
        tableList = new ArrayList<String>();
    }

    @Override
    public void create(String query) {
        // TODO Auto-generated method stub
        Pattern pattern = Pattern
                .compile("SELECT\\s*([DISTINCT]*)(.*) FROM (.*)");
        Matcher matcher = pattern.matcher(query);
        if (matcher.find()) {
            if (!matcher.group(1).isEmpty()) {
                isDistinct = true;
            }
            String rawSelectList = matcher.group(2);
            String rawFromClause = matcher.group(3);
            System.out.println("SELECT Statement: rawSelectList:"
                    + rawSelectList + " rawFromClause:" + rawFromClause
                    + " isDistinct:" + isDistinct);

            selectList = parseCommaSeperatedList(rawSelectList);
            parseFromClause(rawFromClause);
        } else {
            System.out.println("ERROR ::: SELECT statement: Invalid:" + query);
            System.exit(1);
        }
        execute();
    }

    private void parseFromClause(String rawFromClause) {
        String rawTableList = null, rawSearchCond = null;
        Pattern pattern;
        int num = 0;
        if (rawFromClause.contains("WHERE")
                && rawFromClause.contains("ORDER BY")) {
            num = 1;
            pattern = Pattern.compile("(.*) WHERE (.*) ORDER BY (.*)");
        } else if (rawFromClause.contains("WHERE")) {
            num = 2;
            pattern = Pattern.compile("(.*) WHERE (.*)");
        } else if (rawFromClause.contains("ORDER BY")) {
            num = 3;
            pattern = Pattern.compile("(.*) ORDER BY (.*)");
        } else {
            pattern = Pattern.compile("(.*)");
        }
        Matcher matcher = pattern.matcher(rawFromClause);
        if (matcher.find()) {
            rawTableList = matcher.group(1);
            switch (num) {
                case 1:
                    rawSearchCond = matcher.group(2);
                    orderBy = matcher.group(3);
                    break;
                case 2:
                    rawSearchCond = matcher.group(2);
                    break;
                case 3:
                    orderBy = matcher.group(2);
                    break;
            }
            System.out.println("SELECT Statement: rawTableList:" + rawTableList
                    + " rawWhereCond:" + rawSearchCond + " Orderby:" + orderBy);
            tableList = parseCommaSeperatedList(rawTableList);
            if (rawSearchCond != null) {
                cond = new SearchCond();
                cond.create(rawSearchCond);
            }
        } else {
            System.out
                    .println("ERROR ::: SELECT statement: rawTableList Invalid:"
                            + rawFromClause);
            System.exit(1);
        }

    }

    private List<String> parseCommaSeperatedList(String rawList) {
        List<String> list = new ArrayList<String>();

        // Specific to select-list from Select-Statement
        if (rawList.contains("*")) {
            list.add("*");
            return list;
        }
        Pattern pattern = Pattern
                .compile("\\s*([a-z][a-z0-9.]*)\\s*(,*\\s*.*)");
        Matcher matcher = pattern.matcher(rawList);

        while (matcher.find()) {
            String attrName = matcher.group(1);
            list.add(attrName);
            System.out.println("AttrName:" + attrName);
            rawList = matcher.group(2);
            matcher = pattern.matcher(rawList);
        }
        return list;
    }

    @Override
    public void execute() {
// NOTE : WE WILL NEED BELOW STEPS IFF WE NEED TO READ FROM DISK.
//        IF DATA BLOCK COPY ALREADY IN MEMORY, WE NEED TO KEEP TRACK OF IT OURSELF        
//        Relation relation_reference = dbManager.schema_manager.getRelation(tableList.get(0));
//        relation_reference.getBlocks(0, 0, 1);
        ArrayList<Tuple> tuples = dbManager.mem.getTuples(0, 1);
        System.out.println(tuples.get(0));
    }
}
