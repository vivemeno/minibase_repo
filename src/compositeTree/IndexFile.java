package compositeTree;

import java.io.IOException;

import global.RID;

	public abstract class IndexFile 
	{
	  /**
	   * Insert entry into the index file.
	   * @param data the key for the entry
	   * @param rid the rid of the tuple with the key
	   * @exception IOException from lower layers
	   * @exception KeyTooLongException the key is too long
	   * @exception KeyNotMatchException the keys do not match
	   * @exception LeafInsertRecException  insert record to leaf page failed 
	   * @exception IndexInsertRecException insert record to index page failed
	   * @exception ConstructPageException  fail to construct a header page
	   * @exception UnpinPageException unpin page failed
	   * @exception PinPageException  pin page failed
	   * @exception NodeNotMatchException  nodes do not match
	   * @exception ConvertException conversion failed (from global package)
	   * @exception DeleteRecException delete record failed
	   * @exception IndexSearchException index search failed
	   * @exception IteratorException  error from iterator 
	   * @exception LeafDeleteException delete leaf page failed
	   * @exception InsertException insert record failed
	   */
	  abstract public void insert(final KeyClass data, final RID rid)
			  throws IndexInsertRecException, KeyNotMatchException, 
				KeyTooLongException, IOException, ConstructPageException, 
				NodeInsertException, UnpinPageException, PinPageException, DeleteRecException,
				ConvertException, IteratorException, NodeNotMatchException, InsertException, IndexSearchException,ClassNotFoundException, LeafInsertRecException ;
	  
	  /**
	   * Delete entry from the index file.
	   * @param data the key for the entry
	   * @param rid the rid of the tuple with the key
	   * @exception IOException from lower layers
	   * @exception DeleteFashionException delete fashion undefined
	   * @exception LeafRedistributeException failed to redistribute leaf page
	   * @exception RedistributeException redistrubtion failed
	   * @exception InsertRecException insert record failed
	   * @exception KeyNotMatchException keys do not match
	   * @exception UnpinPageException unpin page failed
	   * @exception IndexInsertRecException insert record to index failed
	   * @exception FreePageException  free page failed
	   * @exception RecordNotFoundException failed to find the record
	   * @exception PinPageException pin page failed
	   * @exception IndexFullDeleteException full delete on index page failed
	   * @exception LeafDeleteException delete leaf page failed
	   * @exception IteratorException exception from iterating through records
	   * @exception ConstructPageException fail to construct the header page
	   * @exception DeleteRecException delete record failed
	   * @exception IndexSearchException  index search failed
	   */
	  abstract public boolean Delete(final KeyClass data, final RID rid)  throws
	  KeyNotMatchException, 
	    UnpinPageException, 
	    FreePageException, 
	    RecordNotFoundException, 
	    PinPageException,
	    IndexFullDeleteException, 
	    IteratorException, 
	    ConstructPageException, 
	    IOException, DeleteFashionException, IndexInsertRecException, 
	    IndexSearchException, DeleteRecException, InsertRecException, 
	    LeafRedistributeException, LeafDeleteException, RedistributeException;
}
