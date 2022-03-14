package simpledb.query;

/**
 * The class that denotes values stored in the database.
 * @author Edward Sciore
 */
public class Constant implements Comparable<Constant> {
   private Integer ival = null;
   private String  sval = null;
   private Double dval = null;
   
   public Constant(Integer ival) {
      this.ival = ival;
   }
   
   public Constant(String sval) {
      this.sval = sval;
   }
   
   public Constant(Double dval) {
	   this.dval = dval;
   }
   
   public int asInt() {
      return ival;
   }
   
   public double asDouble() {
	   return dval;
   }
   
   public String asString() {
      return sval;
   }
   
   public boolean equals(Object obj) {
      Constant c = (Constant) obj;
      
      if (ival != null && c.dval!=null) {
    	  return (double)ival == c.dval;
      }
      else if (dval != null && c.ival != null) {
    	  return dval == (double)c.ival;
      }
      
      return (ival != null) ? ival.equals(c.ival) : (dval != null) ? dval.equals(c.dval) : sval.equals(c.sval);
   }
   
   public int compareTo(Constant c) {
      if (ival != null && c.dval!=null) {
    	  if ((double)ival == c.dval)
    		  return 0;
    	  else if ((double)ival < c.dval)
    		  return -1;
    	  else if ((double)ival > c.dval)
    		  return 1;
      }
      else if (dval != null && c.ival != null) {
    	  if (dval == (double)c.ival)
    		  return 0;
    	  else if (dval < (double)c.ival)
    		  return -1;
    	  else if (dval > (double)c.ival)
    		  return 1;
      } 
	   
      return (ival != null) ? ival.compareTo(c.ival) : (dval != null) ? dval.compareTo(c.dval) : sval.compareTo(c.sval);
   }
   
   public int hashCode() {
      return (ival != null) ? ival.hashCode() : sval.hashCode();
   }
   
   public String toString() {
      return (ival != null) ? ival.toString() : sval.toString();
   }  
   
   public boolean isIvalNull() {
	   return ival == null;
   }
   
   public boolean isDvalNull() {
	   return dval == null;
   }
}
