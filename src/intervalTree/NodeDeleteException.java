package intervalTree;

import chainexception.ChainException;

public class NodeDeleteException extends ChainException 
{
  public NodeDeleteException() {super();}
  public NodeDeleteException(Exception e, String s) {super(e,s);}

}
