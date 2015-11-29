package database.physicalquery;

public interface OperatorInterface {
	public void execute();

	public void setNextOperator(OperatorInterface operator);

	public void setBlocksNumbers(int start, int end);
	
	public void setRelationName(String realation_name);
}
