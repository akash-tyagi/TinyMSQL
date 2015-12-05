package database.physicalquery;

import java.util.List;

import storageManager.Tuple;

public interface OperatorInterface {
	public List<Tuple> execute(boolean printResult);

	public void setNextOperator(OperatorInterface operator);

	public void setBlocksNumbers(int start, int end);

	public void setRelationName(String realation_name);
}
