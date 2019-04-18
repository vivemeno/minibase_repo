package intervalTree;

import java.io.IOException;

import diskmgr.Page;
import global.GlobalConst;
import global.PageId;
import global.RID;
import global.SystemDefs;

//implements a search/iterate interface to interval-tree index files
public class IntervalFileScan extends IndexFileScan
implements  GlobalConst{
	
	  IntervalTreeFile iTreefile; 
	  String treeFilename;     // B+ tree we're scanning 
	  ITLeafPage itLeafPage;   // leaf page containing current record
	  RID curRid;       // position in current leaf; note: this is 
	                             // the RID of the key/RID pair within the
	                             // leaf page.                                    
	  boolean didfirst;        // false only before getNext is called
	  boolean deletedcurrent;  // true after deleteCurrent is called (read
	                           // by get_next, written by deleteCurrent).
	    
	  KeyClass endkey;    // if NULL, then go all the way right
	                        // else, stop when current record > this value.
	                        // (that is, implement an inclusive range 
	                        // scan -- the only way to do a search for 
	                        // a single value).
	  int keyType;
	  int maxKeysize;
	  
	  int condition;
	  KeyClass key;
	
	
	 /**
	   * Delete currently-being-scanned(i.e., just scanned)
	   * data entry.
	   *@exception ScanDeleteException  delete error when scan
	   */
	public void delete_current() 

		    throws ScanDeleteException {

		    KeyDataEntry entry;
		    try{  
		      if (itLeafPage == null) {
			System.out.println("No Record to delete!"); 
			throw new ScanDeleteException();
		      }
		      
		      if( (deletedcurrent == true) || (didfirst==false) ) 
			return;    
		      
		      entry=itLeafPage.getCurrent(curRid);  
		      SystemDefs.JavabaseBM.unpinPage( itLeafPage.getCurPage(), false);
		      iTreefile.Delete(entry.key, ((LeafData)entry.data).getData());
		      itLeafPage=iTreefile.findRunStart(entry.key, curRid);
		      
		      deletedcurrent = true;
		      return;
		    }
		    catch (Exception e) {
		      e.printStackTrace();
		      throw new ScanDeleteException();
		    }  
		  }
	
	
	
//	destructor.
//	  * unpin some pages if they are not unpinned already.
//	  * and do some clearing work.
	public void DestroyintervalTreeFileScan() 
			throws  IOException, bufmgr.InvalidFrameNumberException,bufmgr.ReplacerException,
            bufmgr.PageUnpinnedException,bufmgr.HashEntryNotFoundException   
  { 
     if (itLeafPage != null) {
         SystemDefs.JavabaseBM.unpinPage(itLeafPage.getCurPage(), true);
     } 
     iTreefile = null;
     curRid = null;
     endkey = null;
     key = null;
     itLeafPage=null;
  }
	
	
	 /**
	   * Iterate once (during a scan).  
	   *@return null if done; otherwise next KeyDataEntry
	   *@exception ScanIteratorException iterator error
	   */
	public KeyDataEntry get_next() 

		    throws ScanIteratorException
		    {

		    KeyDataEntry entry;
		    PageId nextpage;
		    try {
		      if (itLeafPage == null)
		        return null;
		      
		      if ((deletedcurrent && didfirst) || (!deletedcurrent && !didfirst)) {
		         didfirst = true;
		         deletedcurrent = false;
		         entry=itLeafPage.getCurrent(curRid);
		      }
		      else {
		         entry = itLeafPage.getNext(curRid);
		      }

		      while ( entry == null ) {
		         nextpage = itLeafPage.getNextPage();
		         SystemDefs.JavabaseBM.unpinPage(itLeafPage.getCurPage(), true);
			 if (nextpage.pid == INVALID_PAGE) {
				 itLeafPage = null;
			    return null;
			 }

			 itLeafPage=new ITLeafPage(nextpage, keyType);
			 	
			 entry=itLeafPage.getFirst(curRid);
		      }

		      if (endkey != null)  
		        if ( IntervalT.keyCompare(entry.key, endkey)  > 0) {
		            // went past right end of scan 
			    SystemDefs.JavabaseBM.unpinPage(itLeafPage.getCurPage(), false);
			    itLeafPage=null;
			    return null;
		        }

		      return entry;
		    }
		    catch ( Exception e) {
		         e.printStackTrace();
		         throw new ScanIteratorException();
		    }
		  }
	
	public int keysize() {
		return maxKeysize;
	}
}
