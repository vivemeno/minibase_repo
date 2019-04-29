package compositeTree;

import global.IntervalType;
import global.PageId;
import global.RID;
import compositeTree.IntervalKey;

/** KeyDataEntry: define (key, data) pair.
 */
public class KeyDataEntry {
   /** key in the (key, data)
    */  
   public KeyClass key;
   /** data in the (key, data)
    */
   public DataClass data;
   
  /** Class constructor
   */
  public KeyDataEntry( IntervalType key, PageId pageNo) {
     this.key = new IntervalKey(key); 
     this.data = new IndexData(pageNo);
  }; 



  /** Class constructor.
   */
  public KeyDataEntry(KeyClass key, PageId pageNo) {

     data = new IndexData(pageNo); 
     if ( key instanceof IntervalKey ) 
        this.key= new IntervalKey(((IntervalKey)key).getKey(), ((IntervalKey)key).name);
//     else if ( key instanceof StringKey ) 
//        this.key= new StringKey(((StringKey)key).getKey());    
  };

  /** Class constructor.
   */
  public KeyDataEntry( IntervalType key, RID rid) {
     this.key = new IntervalKey(key); 
     this.data = new LeafData(rid);
  };

  /** Class constructor.
   */
  public KeyDataEntry(KeyClass key, RID rid){
     data = new LeafData(rid); 
     if ( key instanceof IntervalKey ) 
        this.key= new IntervalKey(((IntervalKey)key).getKey(), ((IntervalKey) key).name);
//     else if ( key instanceof StringKey ) 
//        this.key= new StringKey(((StringKey)key).getKey());    
  }; 

  /** Class constructor.
   */
  public KeyDataEntry( KeyClass key,  DataClass data) {
     if ( key instanceof IntervalKey ) 
        this.key= new IntervalKey(((IntervalKey)key).key, ((IntervalKey)key).name);

     if ( data instanceof IndexData ) 
        this.data= new IndexData(((IndexData)data).getData());
     else if ( data instanceof LeafData ) 
        this.data= new LeafData(((LeafData)data).getData()); 
  }

  /** shallow equal. 
   *  @param entry the entry to check again key. 
   *  @return true, if entry == key; else, false.
   */
  public boolean equals(KeyDataEntry entry) {
      boolean st1 = false,st2 =false;

      if ( key instanceof IntervalKey )
         st1= ((IntervalKey)key).getKey().equals
                  (((IntervalKey)entry.key).getKey());
     
      if( data instanceof IndexData )
         st2= ( (IndexData)data).getData().pid==
              ((IndexData)entry.data).getData().pid ;
      else
         st2= ((RID)((LeafData)data).getData()).equals
                (((RID)((LeafData)entry.data).getData()));

  
      return (st1&&st2);
  }     
}

