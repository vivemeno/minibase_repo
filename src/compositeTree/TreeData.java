package compositeTree;

import global.RID;

public class TreeData extends DataClass {
	
	private RID myRid;
	
	TreeData(RID rid) {myRid= new RID(rid.pageNo, rid.slotNo);};  
	
	public RID getData() {
		
		return new RID(myRid.pageNo, myRid.slotNo);
	}
	
	public void setData(RID rid) {
		myRid= new RID(rid.pageNo, rid.slotNo);
	}
	
	public String toString() {
		String s;
	     s="[ "+ (Integer.valueOf(myRid.pageNo.pid)).toString() +" "
	              + (Integer.valueOf(myRid.slotNo)).toString() + " ]";
	     return s;
	}
	
	
}
