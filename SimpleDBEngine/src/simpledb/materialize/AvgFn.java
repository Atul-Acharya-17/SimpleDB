package simpledb.materialize;

import simpledb.query.*;

/**
 * The <i>count</i> aggregation function.
 * @author Edward Sciore
 */
public class AvgFn implements AggregationFn {
   private String fldname;
   private int count;
   private Constant total;
   private Constant avg;
   
   /**
    * Create a count aggregation function for the specified field.
    * @param fldname the name of the aggregated field
    */
   public AvgFn(String fldname) {
      this.fldname = fldname;
   }
   
   /**
    * Start a new count.
    * Since SimpleDB does not support null values,
    * every record will be counted,
    * regardless of the field.
    * The current count is thus set to 1.
    * @see simpledb.materialize.AggregationFn#processFirst(simpledb.query.Scan)
    */
   public void processFirst(Scan s) {
      count = 1;
      
	  if (!s.getVal(fldname).isIvalNull()) {
		  total = new Constant(s.getInt(fldname));
		  avg = new Constant((double)s.getInt(fldname)/ count);
	  }
	  
	  else if (!s.getVal(fldname).isDvalNull()) {
		  total = new Constant(s.getDouble(fldname));
		  avg = new Constant((double)total.asDouble() / count);
	  }
   }
   
   /**
    * Since SimpleDB does not support null values,
    * this method always increments the count,
    * regardless of the field.
    * @see simpledb.materialize.AggregationFn#processNext(simpledb.query.Scan)
    */
   public void processNext(Scan s) {
	   Constant newval = s.getVal(fldname);
	      if (!newval.isDvalNull()) {
	    	  if (newval.asDouble() < Double.parseDouble("1.0E-5"))
	    		  return;
	      }
	   
      count++;
	  if (!s.getVal(fldname).isIvalNull()) {
		  total = new Constant(total.asInt() + s.getInt(fldname));
		  avg = new Constant((double)total.asInt() / count);
	  }
	  
	  else if (!s.getVal(fldname).isDvalNull()) {
		  total = new Constant(total.asDouble() + s.getDouble(fldname));
		  avg = new Constant(total.asDouble() / count);
	  }
   }
   
   /**
    * Return the field's name, prepended by "countof".
    * @see simpledb.materialize.AggregationFn#fieldName()
    */
   public String fieldName() {
      return "avgof" + fldname;
   }
   
   public String originalFldName() {
	   return fldname;
   }
   
   /**
    * Return the current count.
    * @see simpledb.materialize.AggregationFn#value()
    */
   public Constant value() {
      return avg;
   }
}
