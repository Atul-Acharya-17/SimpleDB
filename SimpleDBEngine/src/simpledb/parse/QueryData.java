package simpledb.parse;

import java.util.*;

import simpledb.materialize.AggregationFn;
import simpledb.query.*;

/**
 * Data for the SQL <i>select</i> statement.
 * @author Edward Sciore
 */
public class QueryData {
   private List<String> fields;
   private Collection<String> tables;
   private Predicate pred;
   private List<String> sortfields;
   private List<String> sortorder;
   private List<String> groupbyfields;
   private List<AggregationFn> aggregationfuncs;
   private boolean distinct;
   /**
    * Saves the field and table list and predicate.
    */
   public QueryData(List<String> fields, Collection<String> tables, Predicate pred, List<String>sortfields, List<String>sortorder,
		   List<String> groupByFields, List<AggregationFn> aggregationFuncs, boolean distinct) {
      this.fields = fields;
      this.tables = tables;
      this.pred = pred;
      this.sortfields = sortfields;
      this.sortorder = sortorder;
      this.groupbyfields = groupByFields;
      this.aggregationfuncs = aggregationFuncs;
      this.distinct = distinct;
   }
   
   /**
    * Returns the fields mentioned in the select clause.
    * @return a list of field names
    */
   public List<String> fields() {
      return fields;
   }
   
   /**
    * Returns the tables mentioned in the from clause.
    * @return a collection of table names
    */
   public Collection<String> tables() {
      return tables;
   }
   
   public List<String> sortFields(){
	   return sortfields;
   }
   
   public List<String> sortOrder(){
	   return sortorder;
   }
   
   public List<String> groupByFields(){
	   return groupbyfields;
   }
   
   public List<AggregationFn> aggregationFuncs(){
	   return aggregationfuncs;
   }
   
   public boolean Distinct() {
	   return distinct;
   }
   /**
    * Returns the predicate that describes which
    * records should be in the output table.
    * @return the query predicate
    */
   public Predicate pred() {
      return pred;
   }
   
   public String toString() {
      String result = "select ";
      for (String fldname : fields)
         result += fldname + ", ";
      result = result.substring(0, result.length()-2); //remove final comma
      result += " from ";
      for (String tblname : tables)
         result += tblname + ", ";
      result = result.substring(0, result.length()-2); //remove final comma
      String predstring = pred.toString();
      if (!predstring.equals(""))
         result += " where " + predstring;
      
      if (sortfields.size() > 0) {
    	  result += " order by ";
    	  for (int i=0; i<sortfields.size() - 1; ++i) {
    		  result += sortfields.get(i) + " " + sortorder.get(i) + " ,";
    	  }
    	  result += sortfields.get(sortfields.size() - 1) + " " + sortorder.get(sortorder.size() - 1);
      }
      return result;
   }
}
