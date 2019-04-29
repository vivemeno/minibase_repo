package compositeTree;

import chainexception.ChainException;

public class NodeInsertException extends ChainException 
{
  public NodeInsertException() {super();}
  public NodeInsertException(String s) {super(null,s);}
  public NodeInsertException(Exception e, String s) {super(e,s);}

}
