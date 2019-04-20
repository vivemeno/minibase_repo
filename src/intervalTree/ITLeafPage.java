package intervalTree;

import java.io.IOException;

import diskmgr.Page;
import global.PageId;
import global.RID;


/**
 * A BTLeafPage is a leaf page on a B+ tree.  It holds abstract 
 * <key, RID> pairs; it doesn't know anything about the keys 
 * (their lengths or their types), instead relying on the abstract
 * interface consisting of BT.java.
 */
public class ITLeafPage extends ITSortedPage {
  
  /** pin the page with pageno, and get the corresponding BTLeafPage,
   * also it sets the type to be NodeType.LEAF.
   *@param pageno Input parameter. To specify which page number the
   *  BTLeafPage will correspond to.
   *@param keyType either AttrType.attrInteger or AttrType.attrString.
   *    Input parameter.   
   *@exception IOException  error from the lower layer
   *@exception ConstructPageException BTLeafPage constructor error
   */
  public ITLeafPage(PageId pageno, int keyType) 
    throws IOException, 
	   ConstructPageException
    {
      super(pageno, keyType);
      setType(NodeType.leaf);
    }
  
  /**associate the BTLeafPage instance with the Page instance,
   * also it sets the type to be NodeType.LEAF. 
   *@param page  input parameter. To specify which page  the
   *  BTLeafPage will correspond to.
   *@param keyType either AttrType.attrInteger or AttrType.attrString.
   *  Input parameter.    
   *@exception IOException  error from the lower layer
   *@exception ConstructPageException BTLeafPage constructor error
   */
  public ITLeafPage(Page page, int keyType) 
    throws IOException, 
	   ConstructPageException
    {
      super(page, keyType);
      setType(NodeType.leaf);
    }  
  
  /**new a page, associate the BTLeafPage instance with the Page instance,
   * also it sets the type to be NodeType.LEAF. 
   *@param keyType either AttrType.attrInteger or AttrType.attrString.
   *  Input parameter.
   *@exception IOException  error from the lower layer
   *@exception ConstructPageException BTLeafPage constructor error
   */
  public ITLeafPage( int keyType) 
    throws IOException, 
	   ConstructPageException
    {
      super(keyType);
      setType(NodeType.leaf);
    }  
  

  
  /** insertRecord
   * READ THIS DESCRIPTION CAREFULLY. THERE ARE TWO RIDs
   * WHICH MEAN TWO DIFFERENT THINGS.
   * Inserts a key, rid value into the leaf node. This is
   * accomplished by a call to SortedPage::insertRecord()
   *  Parameters:
   *@param key - the key value of the data record. Input parameter.
   *@param dataRid - the rid of the data record. This is
   *               stored on the leaf page along with the
   *               corresponding key value. Input parameter.
   *
   *@return - the rid of the inserted leaf record data entry,
   *           i.e., the <key, dataRid> pair.
   *@exception  LeafInsertRecException error when insert
   */   
  public RID insertRecord(KeyClass key, RID dataRid) 
    throws  LeafInsertRecException
    {
      KeyDataEntry entry;
      
      if(((IntervalKey)key).name == null) {
      	System.out.println();
      }
      
      try {
        entry = new KeyDataEntry( key,dataRid);
        
        
        return insertRecord(entry);
      }
      catch(Exception e) {
        throw new LeafInsertRecException(e, "insert record failed");
      }
    } // end of insertRecord
  
  
  /**  Iterators. 
   * One of the two functions: getFirst and getNext
   * which  provide an iterator interface to the records on a BTLeafPage.
   *@param rid It will be modified and the first rid in the leaf page
   * will be passed out by itself. Input and Output parameter.
   *@return return the first KeyDataEntry in the leaf page.
   * null if no more record
   *@exception  IteratorException iterator error
   */
  public KeyDataEntry getFirst(RID rid) 
    throws  IteratorException
    {
      
      KeyDataEntry  entry; 
      
      try {
        rid.pageNo = getCurPage();
        rid.slotNo = 0; // begin with first slot
	
        if ( getSlotCnt() <= 0) {
          return null;
        }

        entry=IntervalT.getEntryFromBytes(getpage(), getSlotOffset(0), getSlotLength(0),
				   keyType, NodeType.leaf);
	
        return entry;
      }
      catch (Exception e) {
	throw new IteratorException(e, "Get first entry failed");
      }
    } // end of getFirst

 
   /**Iterators.  
    * One of the two functions: getFirst and getNext which  provide an
    * iterator interface to the records on a BTLeafPage.
    *@param rid It will be modified and the next rid will be passed out 
    *by itself. Input and Output parameter.
    *@return return the next KeyDataEntry in the leaf page. 
    *null if no more record.
    *@exception IteratorException iterator error
    */

   public KeyDataEntry getNext (RID rid)
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
       
       entry=IntervalT.getEntryFromBytes(getpage(),getSlotOffset(i), getSlotLength(i),
                  keyType, NodeType.leaf);
       
       return entry;
     } 
     catch (Exception e) {
       throw new IteratorException(e,"Get next entry failed");
     }
  }
  
  
  
  /**
   * getCurrent returns the current record in the iteration; it is like
   * getNext except it does not advance the iterator.
   *@param rid  the current rid. Input and Output parameter. But
   *    Output=Input.
   *@return return the current KeyDataEntry
   *@exception  IteratorException iterator error
   */ 
   public KeyDataEntry getCurrent (RID rid)
       throws  IteratorException
   {  
     rid.slotNo--;
     return getNext(rid);
   }
  
  
  /** 
   * delete a data entry in the leaf page.
   *@param dEntry the entry will be deleted in the leaf page. Input parameter.
   *@return true if deleted; false if no dEntry in the page
   *@exception LeafDeleteException error when delete
   */
   public boolean delEntry (KeyDataEntry dEntry)
     throws  LeafDeleteException
    {
      KeyDataEntry  entry;
      RID rid=new RID(); 
      
      try {
	for(entry = getFirst(rid); entry!=null; entry=getNext(rid)) 
	  {  
	    if ( entry.equals(dEntry) ) {
	      if ( super.deleteSortedRecord( rid ) == false )
		throw new LeafDeleteException(null, "Delete record failed");
	      return true;
	    }
	    
	 }
	return false;
      } 
      catch (Exception e) {
	throw new LeafDeleteException(e, "delete entry failed");
      }
      
    } // end of delEntry

  /*used in full delete 
   *@param leafPage the sibling page of this. Input parameter.
   *@param parentIndexPage the parant of leafPage and this. Input parameter.
   *@param direction -1 if "this" is left sibling of leafPage ; 
   *      1 if "this" is right sibling of leafPage. Input parameter.
   *@param deletedKey the key which was already deleted, and cause 
   *        redistribution. Input parameter.
   *@exception LeafRedistributeException
   *@return true if redistrbution success. false if we can not redistribute them.
   */
  boolean redistribute(ITLeafPage leafPage, IntervalTPage parentIndexPage, 
		       int direction, KeyClass deletedKey)
    throws LeafRedistributeException
    {
      boolean st;
      // assertion: leafPage pinned
      try {
	if (direction ==-1) { // 'this' is the left sibling of leafPage
	  if ( (getSlotLength(getSlotCnt()-1) + available_space()+ 8 /*  2*sizeof(slot) */) > 
	       ((MAX_SPACE-DPFIXED)/2)) {
            // cannot spare a record for its underflow sibling
            return false;
	  }
	  else {
            // move the last record to its sibling
	    
            // get the last record 
            KeyDataEntry lastEntry;
            lastEntry=IntervalT.getEntryFromBytes(getpage(),getSlotOffset(getSlotCnt()-1)
					   ,getSlotLength(getSlotCnt()-1), keyType, NodeType.leaf);
	    
	    
            //get its sibling's first record's key for adjusting parent pointer
            RID dummyRid=new RID();
            KeyDataEntry firstEntry;
            firstEntry=leafPage.getFirst(dummyRid);

            // insert it into its sibling            
            leafPage.insertRecord(lastEntry);
            
            // delete the last record from the old page
            RID delRid=new RID();
            delRid.pageNo = getCurPage();
            delRid.slotNo = getSlotCnt()-1;
            if ( deleteSortedRecord(delRid) == false )
	      throw new LeafRedistributeException(null, "delete record failed");

	    
            // adjust the entry pointing to sibling in its parent
            if (deletedKey != null)
                st = parentIndexPage.adjustKey(lastEntry.key, deletedKey);
            else 
                st = parentIndexPage.adjustKey(lastEntry.key,
                                            firstEntry.key);
            if (st == false) 
	      throw new LeafRedistributeException(null, "adjust key failed");
            return true;
	  }
	}
	else { // 'this' is the right sibling of pptr
	  if ( (getSlotLength(0) + available_space()+ 8) > ((MAX_SPACE-DPFIXED)/2)) {
            // cannot spare a record for its underflow sibling
            return false;
	  }
	  else {
            // move the first record to its sibling
	    
            // get the first record
            KeyDataEntry firstEntry;
            firstEntry=IntervalT.getEntryFromBytes(getpage(), getSlotOffset(0),
					    getSlotLength(0), keyType,
					    NodeType.leaf);
	    
            // insert it into its sibling
            RID dummyRid=new RID();
            leafPage.insertRecord(firstEntry);
            

            // delete the first record from the old page
            RID delRid=new RID();
            delRid.pageNo = getCurPage();
            delRid.slotNo = 0;
            if ( deleteSortedRecord(delRid) == false) 
	      throw new LeafRedistributeException(null, "delete record failed");  
	    
	    
            // get the current first record of the old page
            // for adjusting parent pointer.
            KeyDataEntry tmpEntry;
            tmpEntry = getFirst(dummyRid);
         
            
            // adjust the entry pointing to itself in its parent
            st = parentIndexPage.adjustKey(tmpEntry.key, firstEntry.key);
            if( st==false) 
	      throw new LeafRedistributeException(null, "adjust key failed"); 
            return true;
	  }
	}
      }
      catch (Exception e) {
	throw new LeafRedistributeException(e, "redistribute failed");
      } 
    } // end of redistribute
  
  
  /* find the position for old key by findKeyData, 
   *  where the  newKey will be returned .
   *@newKey It will replace certain key in index page. Input parameter.
   *@oldKey It helps us to find which key will be replaced by
   * the newKey. Input parameter. 
   *@return false if no key was found; true if success.
   *@exception IndexFullDeleteException delete failed
   */
  
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
  
  /** It inserts a <key, pageNo> value into the index page,
   *@key  the key value in <key, pageNO>. Input parameter. 
   *@pageNo the pageNo  in <key, pageNO>. Input parameter.
   *@return It returns the rid where the record is inserted;
   null if no space left.
   *@exception IndexInsertRecException error when insert
   */
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
  
  /* find entry for key by B+ tree algorithm, 
   * but entry.key may not equal KeyDataEntry.key returned.
   *@param key input parameter.
   *@return return that entry if found; otherwise return null;
   *@exception  IndexSearchException index search failed
   * 
   */
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
  
} // end of BTLeafPage

    
 



