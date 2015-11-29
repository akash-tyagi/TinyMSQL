package database.physicalquery;

import database.DbManager;
import sun.security.pkcs11.Secmod.DbMode;

public class JoinOperator extends OperatorBase implements OperatorInterface {
	String relationName1;
	String relationName2;

	public JoinOperator(String rel1, String rel2, DbManager manager) {
		super(manager);
		relationName1 = rel1;
		relationName2 = rel2;
		this.dbManager = manager;
	}

	public String getResultTableName() {
		return relationName1 + "+" + relationName2;
	}

	@Override
	public void execute() {
		// TODO Auto-generated method stub
	}

	@Override
	public void setNextOperator(OperatorInterface operator) {
		this.next_operator = operator;
	}

}
