package database.physicalquery;

import java.io.PrintWriter;
import java.util.List;

import database.DbManager;
import storageManager.Tuple;
import sun.security.pkcs11.Secmod.DbMode;

public class JoinOperator extends OperatorBase implements OperatorInterface {
	String relationName1;
	String relationName2;

	public JoinOperator(String rel1, String rel2, DbManager manager, PrintWriter writer) {
		super(manager, writer);
		relationName1 = rel1;
		relationName2 = rel2;
		this.dbManager = manager;
	}

	public String getResultTableName() {
		return relationName1 + "+" + relationName2;
	}

	@Override
	public List<Tuple> execute(boolean printResult) {
		return null;
	}

	@Override
	public void setNextOperator(OperatorInterface operator) {
		this.next_operator = operator;
	}

}
