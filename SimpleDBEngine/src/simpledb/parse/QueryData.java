package simpledb.parse;

import java.util.*;

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
   
   /**
    * Saves the field and table list and predicate.
    */
   public QueryData(List<String> fields, Collection<String> tables, Predicate pred, List<String>sortfields, List<String>sortorder) {
      this.fields = fields;
      this.tables = tables;
      this.pred = pred;
      this.sortfields = sortfields;
      this.sortorder = sortorder;
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
