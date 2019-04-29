package compositeTree;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import diskmgr.Page;
import global.*;

//This file contains, among some debug utilities,
//the interface to key and data abstraction.
public class IntervalT implements GlobalConst {
	
	public IntervalT() {
		
	}
	
	/** It gets the length of the key
	   *@param key  specify the key whose length will be calculated.
	   * Input parameter.
	   *@return return the length of the key
	   *@exception  KeyNotMatchException  key is neither StringKey nor  IntegerKey 
	   *@exception IOException   error  from the lower layer  
	   */  
	  protected final static int getKeyLength(KeyClass key)  
		   throws IOException, KeyNotMatchException
	    {
//	      if ( key instanceof IntervalKey) {
//		
//	    ByteArrayOutputStream out = new ByteArrayOutputStream();
//		ObjectOutputStream outstr = new ObjectOutputStream (out); //dataOuputStream write primitve types, and it is faster than ObjectOutputStream
//		outstr.writeObject(((IntervalKey)key).getKey());
//		outstr.flush();
//		outstr.close();
//		int size = out.toByteArray().length;
//		return size;
//	    }
//	      else
	    	  if ( key instanceof IntervalKey)
		return GlobalConst.COMPOSITE_KEY_LEN;
	      else throw new KeyNotMatchException(null, "key types do not match intervalType"); 
	    }
	
	public static int keyCompare(KeyClass key1, KeyClass key2)throws KeyNotMatchException
    {
	      if ((key1 instanceof IntervalKey) && (key2 instanceof IntervalKey) ) {
	    	  IntervalKey k1 = (IntervalKey) key1;
	    	  IntervalKey k2 = (IntervalKey) key2;
	    	  if(k1.name.compareTo(k2.name) > 0) {  
	    		  return 1;
	    	  }
	    	  else if (k1.name.compareTo(k2.name) == 0) {
	    		  return k1.key.s - k2.key.s;
	    	  } else {
	    		  return -1;
	    	  }
	      }
	      
	      else { throw new  KeyNotMatchException(null, "key types do not match");}
	    } 

//	For debug. Print the B+ tree structure out
	public static void printintervalTree(IntervalTreeHeaderPage header) throws IOException, 
	   HashEntryNotFoundException,
	   InvalidFrameNumberException,
	   PageUnpinnedException,
	   ReplacerException, ConstructPageException, IteratorException 
	 {
	   if(header.get_rootId().pid == INVALID_PAGE) {
		System.out.println("The Tree is Empty!!!");
		return;
	   }
	   
	   System.out.println("");
	   System.out.println("");
	   System.out.println("");
	   System.out.println("---------------The Interval Tree Structure---------------");
	   
	   
	   System.out.println(1+ "           "+header.get_rootId());
	   
	   _printTree(header.get_rootId(), "     ", 1, header.get_keyType());
	   
	   System.out.println("--------------- End ---------------");
	   System.out.println("");
	   System.out.println("");
	 }
	
	 private static void _printTree(PageId currentPageId, String prefix, int i, int keyType) 
		throws IOException, 
		   HashEntryNotFoundException,
		   InvalidFrameNumberException,
		   PageUnpinnedException,
		   ReplacerException, ConstructPageException, IteratorException 
		{
		  
		 ITSortedPage sortedPage=new ITSortedPage(currentPageId, keyType);
		  prefix=prefix+"       ";
		  i++;
				  if( sortedPage.getType()==NodeType.node) {  
					  IntervalTPage indexPage=new IntervalTPage((Page)sortedPage, keyType);
				
				System.out.println(i+prefix+ indexPage.getPrevPage());
				_printTree( indexPage.getPrevPage(), prefix, i, keyType);
				
				RID rid=new RID();
				for( KeyDataEntry entry=indexPage.getFirst(rid); entry!=null; 
				     entry=indexPage.getNext(rid)) {
				  System.out.println(i+prefix+(IndexData)entry.data);
				  _printTree( ((IndexData)entry.data).getData(), prefix, i, keyType);
				}
				  }
		  SystemDefs.JavabaseBM.unpinPage(currentPageId , true/*dirty*/);
		}
	
//	used for debug: to print a page out. The page is either BTIndexPage,
//	   * or BTLeafPage. 
//	   *@param pageno the number of page. Input parameter.
	static void printPage(PageId pageno) throws  IOException, 
	    IteratorException, 
	    ConstructPageException,
	        HashEntryNotFoundException, 
	    ReplacerException, 
	    PageUnpinnedException, 
	        InvalidFrameNumberException
	{
		int keyType = 5;
	  ITSortedPage sortedPage=new ITSortedPage(pageno, keyType);
	  int i;
	  i=0;
	  if ( sortedPage.getType()==NodeType.node ) {
		  IntervalTPage indexPage=new IntervalTPage((Page)sortedPage, keyType);
	    System.out.println("");
	    System.out.println("**************To Print an Index Page ********");
	    System.out.println("Current Page ID: "+ indexPage.getCurPage().pid);
	    System.out.println("Left Link      : "+ indexPage.getLeftLink().pid);
	
	    RID rid=new RID();
	
	    for(KeyDataEntry entry=indexPage.getFirst(rid); entry!=null; 
	    entry=indexPage.getNext(rid)){
	  if( keyType==AttrType.attrInterval) 
	    System.out.println(i+" (key, pageId):   ("+ 
			       (IntervalKey)entry.key + ",  "+(IndexData)entry.data+ " )");
	  
	  i++;    
	    }
	
	    System.out.println("************** END ********");
	    System.out.println("");
	  }
	  else if ( sortedPage.getType()==NodeType.leaf ) {
	    ITLeafPage leafPage=new ITLeafPage((Page)sortedPage, keyType);
	    System.out.println("");
	    System.out.println("**************To Print an Leaf Page ********");
	    System.out.println("Current Page ID: "+ leafPage.getCurPage().pid);
	    System.out.println("Left Link      : "+ leafPage.getPrevPage().pid);
	    System.out.println("Right Link     : "+ leafPage.getNextPage().pid);
	
	    RID rid=new RID();
	
	    for(KeyDataEntry entry=leafPage.getFirst(rid); entry!=null; 
	    entry=leafPage.getNext(rid)){
	  if( keyType==AttrType.attrInterval) 
	    System.out.println(i+" (key, [pageNo, slotNo]):   ("+ 
			       (IntervalKey)entry.key+ ",  "+(LeafData)entry.data+ " )");
	  
	  i++;
	    }
	
	System.out.println("************** END ********");
	System.out.println("");
	  }
	  else {
	System.out.println("Sorry!!! This page is neither Index nor Leaf page.");
	  }      
	  
	  SystemDefs.JavabaseBM.unpinPage(pageno, true/*dirty*/);
	}
	
	
	static void printTreeUtilization(IntervalTreeHeaderPage header) {
		
	}
	
	static void printNonLeafTreeUtilization(IntervalTreeHeaderPage header) {
		
	}
	
	/** It convert a keyDataEntry to byte[].
	   *@param  entry specify  the data entry. Input parameter.
	   *@return return a byte array with size equal to the size of (key,data). 
	   *@exception   KeyNotMatchException  entry.key is neither StringKey nor  IntegerKey
	   *@exception NodeNotMatchException entry.data is neither LeafData nor IndexData
	   *@exception ConvertException error from the lower layer
	   */
	protected final static byte[] getBytesFromEntry( KeyDataEntry entry ) 
		    throws KeyNotMatchException, 
			   NodeNotMatchException, 
			   ConvertException
		    {
		      byte[] data;
		      int n, m;
		      try{
		        n=getKeyLength(entry.key);
		        m=n;
		        if( entry.data instanceof IndexData )
			  n+=4;
		        else if (entry.data instanceof LeafData )      
			  n+=8;
			
		        data=new byte[n];
			
		        if ( entry.key instanceof IntervalKey ) {
			  Convert.setNodelValue( ((IntervalKey)entry.key),
					       0, data);
		        }
		        else throw new KeyNotMatchException(null, "key types do not match");
		        
		        if ( entry.data instanceof IndexData ) {
			  Convert.setIntValue( ((IndexData)entry.data).getData().pid,
					       m, data);
		        }
		        else if ( entry.data instanceof LeafData ) {
			  Convert.setIntValue( ((LeafData)entry.data).getData().slotNo,
					       m, data);
			  Convert.setIntValue( ((LeafData)entry.data).getData().pageNo.pid,
					       m+4, data);
			  
		        }
		        else throw new NodeNotMatchException(null, "node types do not match");
		        return data;
		      } 
		      catch (IOException e) {
		        throw new  ConvertException(e, "convert failed");
		      }
		    } 
	
	/** It gets an keyDataEntry from bytes array and position
	   *@param from  It's a bytes array where KeyDataEntry will come from. 
	   * Input parameter.
	   *@param offset the offset in the bytes. Input parameter.
	   *@param keyType It specifies the type of key. It can be 
	   *               AttrType.attrString or AttrType.attrInteger.
	   *               Input parameter. 
	   *@param nodeType It specifes NodeType.LEAF or NodeType.INDEX. 
	   *                Input parameter.
	   *@param length  The length of (key, data) in byte array "from".
	   *               Input parameter.
	   *@return return a KeyDataEntry object
	   *@exception KeyNotMatchException  key is neither StringKey nor  IntegerKey
	   *@exception NodeNotMatchException  nodeType is neither NodeType.LEAF 
	   *  nor NodeType.INDEX.
	   *@exception ConvertException  error from the lower layer 
	 * @throws ClassNotFoundException 
	   */
	  protected final static 
	      KeyDataEntry getEntryFromBytes( byte[] from, int offset,  
					      int length, int keyType, short nodeType )
	    throws KeyNotMatchException, 
		   NodeNotMatchException, 
		   ConvertException, ClassNotFoundException
	    {
	      KeyClass key;
	      DataClass data;
	      int n;
	      try {
	    	  int b = from.length;
		
		if ( nodeType==NodeType.index ) {
		  n=4;
		  data= new IndexData( Convert.getIntValue(offset+length-4, from));
		}
		else if ( nodeType==NodeType.leaf) {
		  n=8;
		  RID rid=new RID();
		  rid.slotNo =   Convert.getIntValue(offset+length-8, from);
		  rid.pageNo =new PageId();
		  rid.pageNo.pid= Convert.getIntValue(offset+length-4, from); 
		  data = new LeafData(rid);
		}
		else throw new NodeNotMatchException(null, "node types do not match"); 
		
		if ( keyType== AttrType.attrInterval) {
			NodeTable tmpTB = Convert.getNodeValue(offset, from, length-n);
		  key= new IntervalKey(
				  tmpTB.interval, tmpTB.nodename);
		}
		else 
	          throw new KeyNotMatchException(null, "key types do not match");
		
		return new KeyDataEntry(key, data);
		
	      } 
	      catch ( IOException e) {
		throw new ConvertException(e, "convert faile");
	      }
	    } 
	  
	  /** It gets the length of the (key,data) pair in leaf or index page. 
	   *@param  key    an object of btree.KeyClass.  Input parameter.
	   *@param  pageType  NodeType.LEAF or  NodeType.INDEX. Input parameter.
	   *@return return the lenrth of the (key,data) pair.
	   *@exception  KeyNotMatchException key is neither StringKey nor  IntegerKey 
	   *@exception NodeNotMatchException pageType is neither NodeType.LEAF 
	   *  nor NodeType.INDEX.
	   *@exception IOException  error from the lower layer 
	   */ 
	  protected final static int getKeyDataLength(KeyClass key, short pageType ) 
	    throws KeyNotMatchException, 
		   NodeNotMatchException, 
		   IOException
	    {
	      return getKeyLength(key) + getDataLength(pageType);
	    } 
	  
	  /** It gets the length of the data 
	   *@param  pageType  NodeType.LEAF or  NodeType.INDEX. Input parameter.
	   *@return return 8 if it is of NodeType.LEA; 
	   *  return 4 if it is of NodeType.INDEX.
	   *@exception  NodeNotMatchException pageType is neither NodeType.LEAF 
	   *  nor NodeType.INDEX.
	   */  
	  protected final static int getDataLength(short pageType) 
	    throws  NodeNotMatchException
	    {
	      if ( pageType==NodeType.leaf)
		return 8;
	      else if ( pageType==NodeType.index)
		return 4;
	      else throw new  NodeNotMatchException(null, "key types do not match"); 
	    }
	
}
