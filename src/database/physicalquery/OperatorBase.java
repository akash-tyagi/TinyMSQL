package database.physicalquery;

import java.util.ArrayList;
import java.util.List;

import database.DbManager;
import storageManager.Tuple;

public class OperatorBase {
	DbManager dbManager;
	OperatorInterface next_operator;
	boolean isReadFromMem;
	String relation_name;
	List<Tuple> res_tuples;

	int start_block;
	int end_block;

	public OperatorBase(DbManager dbManager) {
		this.dbManager = dbManager;
		res_tuples = new ArrayList<Tuple>();
	}

	public void setNextOperator(OperatorInterface operator) {
		this.next_operator = operator;
	}

	public void setBlocksNumbers(int start, int end) {
		start_block = start;
		end_block = end;
		isReadFromMem = true;
	}

	public void setRelationName(String realation_name) {
		this.relation_name = realation_name;
	}
}
