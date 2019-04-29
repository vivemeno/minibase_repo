package compositeTree;

import compositeTree.DataClass;
import global.PageId;
import global.RID;

public class NodeData extends DataClass {
	private PageId pageId;

	  public String toString() {
	     return (new Integer(pageId.pid)).toString();
	  }

	  /** Class constructor
	   *  @param     pageNo  the page number
	   */
	  NodeData(PageId  pageNo) { pageId = new PageId(pageNo.pid);};  

	  /** Class constructor
	   *  @param     pageNo  the page number
	   */
	  NodeData(int  pageNo) { pageId = new PageId(pageNo);};  


	  /** get a copy of the pageNo
	  *  @return the reference of the copy 
	  */
	  protected PageId getData() {return new PageId(pageId.pid); };

	  /** set the pageNo 
	   */ 
	  protected void setData(PageId pageNo) {pageId= new PageId(pageNo.pid);};
	}   
