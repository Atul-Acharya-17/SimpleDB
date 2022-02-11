package simpledb.materialize;

import java.util.*;

import simpledb.query.*;

/**
 * A comparator for scans.
 * @author Edward Sciore
 */
public class RecordComparator implements Comparator<Scan> {
   private List<String> fields;
   private List<String> order;
   
   /**
    * Create a comparator using the specified fields,
    * using the ordering implied by its iterator.
    * @param fields a list of field names
    */
   
   public RecordComparator(List<String> fields) {
	      this.fields = fields;
	      this.order = new ArrayList<String>();
	   }
   
   public RecordComparator(List<String> fields, List<String> order) {
      this.fields = fields;
      this.order = order;
   }
   
   /**
    * Compare the current records of the two specified scans.
    * The sort fields are considered in turn.
    * When a field is encountered for which the records have
    * different values, those values are used as the result
    * of the comparison.
    * If the two records have the same values for all
    * sort fields, then the method returns 0.
    * @param s1 the first scan
    * @param s2 the second scan
    * @return the result of comparing each scan's current record according to the field list
    */
   public int compare(Scan s1, Scan s2) {
	  int idx = 0;
      for (String fldname : fields) {
         Constant val1 = s1.getVal(fldname);
         Constant val2 = s2.getVal(fldname);
         int result = val1.compareTo(val2);
         
         if (order.size() > 0 && order.get(idx).equals("desc"))
        	 result *= -1;
         
         if (result != 0)
            return result;
         
         ++idx;
      }
      return 0;
   }
}
