package simpledb.materialize;

import simpledb.query.*;

/**
 * The <i>max</i> aggregation function.
 * @author Edward Sciore
 */
public class SumFn implements AggregationFn {
   private String fldname;
   private Constant total;
   
   /**
    * Create a max aggregation function for the specified field.
    * @param fldname the name of the aggregated field
    */
   public SumFn(String fldname) {
      this.fldname = fldname;
   }
   
   /**
    * Start a new maximum to be the 
    * field value in the current record.
    * @see simpledb.materialize.AggregationFn#processFirst(simpledb.query.Scan)
    */
   public void processFirst(Scan s) {
		  if (!s.getVal(fldname).isIvalNull()) {
			  total = new Constant(s.getInt(fldname));
		  }
		  
		  else if (!s.getVal(fldname).isDvalNull()) {
			  total = new Constant(s.getDouble(fldname));
		  }
   }
   
   /**
    * Replace the current maximum by the field value
    * in the current record, if it is higher.
    * @see simpledb.materialize.AggregationFn#processNext(simpledb.query.Scan)
    */
   public void processNext(Scan s) {
	  if (!s.getVal(fldname).isIvalNull()) {
		  total = new Constant(total.asInt() + s.getInt(fldname));
	  }
	  
	  else if (!s.getVal(fldname).isDvalNull()) {
		  total = new Constant(total.asDouble() + s.getDouble(fldname));
	  }
   }
   
   /**
    * Return the field's name, prepended by "maxof".
    * @see simpledb.materialize.AggregationFn#fieldName()
    */
   public String fieldName() {
      return "sumof" + fldname;
   }
   
   public String originalFldName() {
	   return fldname;
   }
   
   
   /**
    * Return the current maximum.
    * @see simpledb.materialize.AggregationFn#value()
    */
   public Constant value() {
      return total;
   }
}
