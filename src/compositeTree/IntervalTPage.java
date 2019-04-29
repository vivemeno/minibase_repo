package compositeTree;

import java.io.IOException;


import compositeTree.KeyDataEntry;
import compositeTree.NodeType;
import compositeTree.ConstructPageException;
import global.AttrType;
import diskmgr.Page;
import global.Convert;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.HFPage;
import heap.InvalidSlotNumberException;

//A page on an interval-tree.
public class IntervalTPage extends ITSortedPage {
	
	int keyType=5; //default to intervalType
	KeyDataEntry currentKeyDataEntry = null;   
	
	/*
	 * pin the page with pageno, and get the corresponding BTIndexPage, also it sets
	 * the type of node to be NodeType.INDEX.
	 * 
	 * @param pageno Input parameter. To specify which page number the BTIndexPage
	 * will correspond to.
	 * 
	 * @param keyType either AttrType.attrInteger or AttrType.attrString. Input
	 * parameter.
	 */	public IntervalTPage(PageId pageno, int keyType) throws ConstructPageException, IOException 
		    {
		 	  super(pageno, keyType);
		      setType(NodeType.node);
		    } 
	
	public IntervalTPage(Page page, int keyType) 
			throws IOException, 
			   ConstructPageException
		    {
			super(page, keyType);
	      setType(NodeType.index);
		    }  
	
	public IntervalTPage( int keyType) 
		    throws IOException, 
			   ConstructPageException
		    {
		      super(keyType);
		      setType(NodeType.index);
		    }    
	
	/*
	 * OPTIONAL: fullDeletekey This is optional, and is only needed if you want to
	 * do full deletion. Return its RID. delete key may != key. But delete key <=
	 * key, and the delete key is the first biggest key such that delete key <= key
	 * 
	 * @param key the key used to search. Input parameter.
	 * 
	 * @exception IndexFullDeleteException if no record deleted or failed by any
	 * reason
	 * 
	 * @return RID of the record deleted. Can not return null.
	 */	
	public boolean delEntry(KeyDataEntry dEntry)throws IndexFullDeleteException, NodeDeleteException 
	{
	      KeyDataEntry  entry;
	      RID rid=new RID(); 
	      
	      try {
		for(entry = getFirst(rid); entry!=null; entry=getNext(rid)) 
		  {  
		    if ( entry.equals(dEntry) ) {
		      if ( super.deleteSortedRecord( rid ) == false )
			throw new NodeDeleteException(null, "Delete record failed");
		      return true;
		    }
		    
		 }
		return false;
	      } 
	      catch (Exception e) {
		throw new NodeDeleteException(e, "delete entry failed");
	      }
	      
	    } // end of deleteKey
	
	public KeyDataEntry getCurrent(RID rid) {
		
		return currentKeyDataEntry;
	}
	
	/*
	 * Iterators. One of the two functions: getFirst and getNext which provide an
	 * iterator interface to the records on a BTIndexPage.
	 * 
	 * @param rid It will be modified and the first rid in the index page will be
	 * passed out by itself. Input and Output parameter.
	 * 
	 * @return return the first KeyDataEntry in the index page. null if NO MORE
	 * RECORD
	 * 
	 * @exception IteratorException iterator error
	 */	public KeyDataEntry getFirst(RID rid)
			throws IteratorException
    {
      
      KeyDataEntry  entry; 
      
      try { 
	rid.pageNo = getCurPage();
	rid.slotNo = 0; // begin with first slot
	
	if ( getSlotCnt() == 0) {
	  return null;
	}
	
	entry=IntervalT.getEntryFromBytes( getpage(),getSlotOffset(0), 
				    getSlotLength(0),
				    keyType, NodeType.node);
	
	return entry;
      } 
      catch (Exception e) {
	throw new IteratorException(e, "Get first entry failed");
      }
      
    } // end of getFirst
	
	 
	/*
	 * Iterators. One of the two functions: get_first and get_next which provide an
	 * iterator interface to the records on a BTIndexPage.
	 * 
	 * @param rid It will be modified and next rid will be passed out by itself.
	 * Input and Output parameter.
	 * 
	 * @return return the next KeyDataEntry in the index page. null if no more
	 * record
	 * 
	 * @exception IteratorException iterator error
	 */
	public KeyDataEntry getNext(RID rid) 
			throws  IteratorException 
    {
      KeyDataEntry  entry; 
      int i;
      try{
	rid.slotNo++; //must before any return;
	i=rid.slotNo;
	
	if ( rid.slotNo >= getSlotCnt())
	  {
	    return null;
	  }
	
	entry=IntervalT.getEntryFromBytes(getpage(),getSlotOffset(i), 
				   getSlotLength(i),
				   keyType, NodeType.node);
	
	return entry;
      } 
      catch (Exception e) {
	throw new IteratorException(e, "Get next entry failed");
      }
    } // end of getNext
	
	
	public RID insertRecord(KeyClass key, RID dataRid) throws IOException, NodeInsertException 
	 {
	      KeyDataEntry entry;
	      
	      try {
	        entry = new KeyDataEntry( key,dataRid);
		
	        return insertRecord(entry);
	      }
	      catch(Exception e) {
	        throw new NodeInsertException(e, "insert record failed");
	      }
	    } // end of insertRecord
	
	//not sure
	public void ITDeleteRecord(RID rid) 
			throws DeleteRecException
	    {
	      try {
		
		deleteRecord(rid);
		compact_slot_dir();
		return;
		// ASSERTIONS:
		// - slot directory is compacted
	      }
	      catch (Exception  e) {
		if (e instanceof InvalidSlotNumberException)
		  return;
		else
		  throw new DeleteRecException(e, "delete record failed");
	      }
	    } // end of deleteSortedRecord
	
	public RID insertKey(KeyClass key, PageId pageNo) 
		      throws  IndexInsertRecException
		    {
		      RID rid;
		      KeyDataEntry entry;
		      try {
		        entry = new KeyDataEntry( key, pageNo); 
		        rid=super.insertRecord( entry );
		        return rid;
		      }
		      catch ( Exception e) {       
		        throw new IndexInsertRecException(e, "Insert failed");
		        
		      }
		    }
	
	boolean adjustKey(KeyClass newKey, KeyClass oldKey) 
		    throws IndexFullDeleteException
		    {
		      
		      try {
		        
			KeyDataEntry entry;
			entry =  findKeyData( oldKey );
			if (entry == null) return false;
			
			RID rid=deleteKey( entry.key );
			if (rid==null) throw new IndexFullDeleteException(null, "Rid is null");
			
			rid=insertKey( newKey, ((IndexData)entry.data).getData());        
			if (rid==null) throw new IndexFullDeleteException(null, "Rid is null");
			
			return true;
		      }
		      catch (Exception e) {
		        throw new IndexFullDeleteException(e, "Adjust key failed");
		      }
		    } // end of adjustKey
	
	KeyDataEntry findKeyData(KeyClass key) 
		    throws IndexSearchException
		    {
		      KeyDataEntry entry;
		      
		      try {  
			
		        for (int i = getSlotCnt()-1; i >= 0; i--) {
			  entry=IntervalT.getEntryFromBytes(getpage(),getSlotOffset(i), 
						     getSlotLength(i), keyType, NodeType.node);
			  
			  if (IntervalT.keyCompare(key, entry.key) >= 0) {
			    return entry;
			  }
		        }
		        return null;
		      }
		      catch ( Exception e) {
		        throw  new IndexSearchException(e, "finger key data failed");
		      }
		    } // end of findKeyData   
	
	/*  OPTIONAL: fullDeletekey 
	   * This is optional, and is only needed if you want to do full deletion.
	   * Return its RID.  delete key may != key.  But delete key <= key,
	   * and the delete key is the first biggest key such that delete key <= key 
	   *@param key the key used to search. Input parameter.
	   *@exception IndexFullDeleteException if no record deleted or failed by
	   * any reason
	   *@return  RID of the record deleted. Can not return null.
	   */
	  RID deleteKey(KeyClass key) 
	    throws IndexFullDeleteException 
	    {
	      KeyDataEntry  entry;
	      RID rid=new RID(); 
	      
	      
	      try {
		
		entry = getFirst(rid);
		
		if (entry == null) 
		  //it is supposed there is at least a record
		  throw new IndexFullDeleteException(null, "No records found");
		
		
		if ( IntervalT.keyCompare(key, entry.key)<0 )  
		  //it is supposed to not smaller than first key
		  throw new IndexFullDeleteException(null, "First key is bigger");
		
		
		while (IntervalT.keyCompare(key, entry.key) > 0) {
		  entry = getNext(rid );
		  if (entry == null)
	            break;
		}
		
		if (entry == null) rid.slotNo--;
		else if (IntervalT.keyCompare(key, entry.key) != 0)
		  rid.slotNo--; // we want to delete the previous key
		
		deleteSortedRecord(rid);
		return rid;
	      }
	      catch (Exception e) {
	        throw new IndexFullDeleteException(e, "Full delelte failed"); 
	      }
	    } // end of deleteKey
	  
	  /* 
	   * This function encapsulates the search routine to search a
	   * BTIndexPage by B++ search algorithm 
	   *@param key  the key value used in search algorithm. Input parameter. 
	   *@return It returns the page_no of the child to be searched next.
	   *@exception IndexSearchException Index search failed;
	   */
	  PageId getPageNoByKey(KeyClass key) 
	    throws IndexSearchException         
	    {
	      KeyDataEntry entry;
	      int i;
	      
	      try {
		
		for (i=getSlotCnt()-1; i >= 0; i--) {
		  entry= IntervalT.getEntryFromBytes( getpage(),getSlotOffset(i), 
					       getSlotLength(i), keyType, NodeType.node);
		  
		  if (IntervalT.keyCompare(key, entry.key) >= 0)
		    {
		      return ((IndexData)entry.data).getData();
		    }
		}
		
		return getPrevPage();
	      } 
	      catch (Exception e) {
		throw new IndexSearchException(e, "Get entry failed");
	      }   
	      
	    } // getPageNoByKey
	  
	  /*It is used in full delete
	   *@param key the key is used to search. Input parameter. 
	   *@param pageNo It returns the pageno of the sibling. Input and Output
	   *       parameter.
	   *@return 0 if no sibling; -1 if left sibling; 1 if right sibling.
	   *@exception IndexFullDeleteException delete failed
	   */  
	  
	  int  getSibling(KeyClass key, PageId pageNo)
	    throws IndexFullDeleteException
	    {
	      
	      try {
		if (getSlotCnt() == 0) // there is no sibling 
	          return 0;
		
		int i;
		KeyDataEntry entry;
		for (i=getSlotCnt()-1; i >= 0; i--) {
		  entry=IntervalT.getEntryFromBytes(getpage(), getSlotOffset(i),
					     getSlotLength(i), keyType , NodeType.node);
		  if (IntervalT.keyCompare(key, entry.key)>=0) {
		    if (i != 0) { 
	              entry=IntervalT.getEntryFromBytes(getpage(), getSlotOffset(i-1),
						 getSlotLength(i-1), keyType, NodeType.node);
	              pageNo.pid=  ((IndexData)entry.data).getData().pid;
	              return -1; //left sibling
		    }
		    else {
	              pageNo.pid = getLeftLink().pid;
	              return -1; //left sibling
		    }
		  }
		}
		entry=IntervalT.getEntryFromBytes(getpage(), getSlotOffset(0),
					   getSlotLength(0), keyType, NodeType.node);    
		pageNo.pid=  ((IndexData)entry.data).getData().pid; 
		return 1;  //right sibling
	      }
	      catch (Exception e) {
		throw new IndexFullDeleteException(e, "Get sibling failed");
	      }
	    } // end of getSibling  
	  
	  /** Left Link 
	   *  You will recall that the index pages have a left-most
	   *  pointer that is followed whenever the search key value
	   *  is less than the least key value in the index node. The
	   *  previous page pointer is used to implement the left link.
	   *@return It returns the left most link. 
	   *@exception IOException error from the lower layer
	   */
	  protected PageId getLeftLink() 
	    throws IOException
	    {
	      return getPrevPage(); 
	    }
	  
	  /* find a key  by B++ algorithm, 
	   * but returned key may not equal the key passed in.
	   *@param key input parameter.
	   *@return return that key if found; otherwise return null;
	   *@exception  IndexSearchException index search failed 
	   * 
	   */
	  KeyClass findKey(KeyClass key) 
	    throws IndexSearchException
	    {
	      return findKeyData(key).key;
	    }
	
	  /*It is used in full delete
	   *@param indexPage the sibling page of this. Input parameter.
	   *@param parentIndexPage the parant of indexPage and this. Input parameter.
	   *@param direction -1 if "this" is left sibling of indexPage ; 
	   *      1 if "this" is right sibling of indexPage. Input parameter.
	   *@param deletedKey the key which was already deleted, and cause 
	   *     redistribution. Input parameter.
	   *@exception RedistributeException Redistribution failed
	   *@return true if redistrbution success. false if we can not redistribute them.
	   */
	  boolean redistribute(IntervalTPage indexPage, IntervalTPage parentIndexPage,
			       int direction, KeyClass deletedKey)
	    throws RedistributeException
	    {
	      
	      // assertion: indexPage and parentIndexPage are  pinned
	      try {  
		boolean st;
		if (direction==-1) { // 'this' is the left sibling of indexPage
		  if (( getSlotLength(getSlotCnt()-1) + available_space()) > 
		      ( (MAX_SPACE-DPFIXED)/2) ) {
	            // cannot spare a record for its underflow sibling
	            return false;
		  }
		  else {
	            // get its sibling's first record's key 
	            RID dummyRid=new RID();
	            KeyDataEntry firstEntry, lastEntry;
	            firstEntry=indexPage.getFirst(dummyRid);
	            
	            // get the entry pointing to the right sibling
		    
	            KeyClass splitKey = parentIndexPage.findKey(firstEntry.key);
		    
	            // get the leftmost child pointer of the right sibling
	            PageId leftMostPageId = indexPage.getLeftLink();
	            
	            // insert  <splitKey,leftMostPageId>  to its sibling
	            indexPage.insertKey( splitKey, leftMostPageId);

	            // get the last record of itself
	            lastEntry=IntervalT.getEntryFromBytes(getpage(), getSlotOffset(getSlotCnt()-1),     
						   getSlotLength(getSlotCnt()-1), keyType, NodeType.node);
		    
	            // set sibling's leftmostchild to be lastPageId
	            indexPage.setLeftLink(((IndexData)(lastEntry.data)).getData() );
		    
	            // delete the last record from the old page
	            RID delRid=new RID();
	            delRid.pageNo = getCurPage();
	            delRid.slotNo = getSlotCnt()-1;

	            if ( deleteSortedRecord(delRid) ==false )
	                   throw new RedistributeException(null, "Delete record failed");

	            // adjust the entry pointing to sibling in its parent
	            if (deletedKey!=null )
	                st = parentIndexPage.adjustKey( lastEntry.key, deletedKey);
		    
	            else 
		      st = parentIndexPage.adjustKey( lastEntry.key, splitKey);
	            if (st==false)
		      throw new RedistributeException(null, "adjust key failed");
	            return true;
		  }
		}
		else { // 'this' is the right sibling of indexPage
		  if ( (getSlotLength(0) + available_space()) > ((MAX_SPACE-DPFIXED)/2) ) {
	            // cannot spare a record for its underflow sibling
	            return false;
		  }
		  else {
	            // get the first record
	            KeyDataEntry firstEntry; 
	            firstEntry=IntervalT.getEntryFromBytes( getpage(),
						     getSlotOffset(0),
						     getSlotLength(0), keyType, NodeType.node);
		    
	            // get its leftmost child pointer
	            PageId leftMostPageId = getLeftLink();
		    
	            // get the entry in its parent pointing to itself
	            KeyClass splitKey;
	            splitKey = parentIndexPage.findKey(firstEntry.key);
		    
	            // insert <split, leftMostPageId> to its left sibling 
		    
	            indexPage.insertKey(splitKey,leftMostPageId);
	            
		    
	            // set its new leftmostchild
	            setLeftLink(((IndexData)(firstEntry.data)).getData());
	            
	            // delete the first record 
	            RID delRid=new RID();
	            delRid.pageNo = getCurPage();
	            delRid.slotNo = 0;
	            if (deleteSortedRecord(delRid) == false )
		      throw new RedistributeException(null, "delete record failed");
		    
	            // adjust the entry pointing to itself in its parent
	            if ( parentIndexPage.adjustKey(firstEntry.key, splitKey) ==false )
		      throw new RedistributeException(null, "adjust key failed");  
	            return true;
		  }
		} //else
	      } //try
	      catch (Exception e){
		throw new RedistributeException(e, "redistribute failed");
	      } 
	    } // end of redistribute 
	  
	  /** You will recall that the index pages have a left-most
	   *  pointer that is followed whenever the search key value
	   *  is less than the least key value in the index node. The
	   *  previous page pointer is used to implement the left link.
	   * The function sets the left link.
	   *@param left the PageId of the left link you wish to set. Input parameter.
	   *@exception IOException I/O errors
	   */ 
	  protected void setLeftLink(PageId left) 
	    throws IOException
	    { 
	      setPrevPage(left); 
	    }
	  
}
