package simpledb.nested;


import simpledb.query.Constant;
import simpledb.query.Scan;

public class NestedLoopScan implements Scan {
	private Scan s1;
	private Scan s2;
	private String fldname1, fldname2;
	
    public NestedLoopScan(Scan s1, Scan s2, String fldname1, String fldname2) {
    	this.s1 = s1;
    	this.s2 = s2;
    	this.fldname1 = fldname1;
    	this.fldname2 = fldname2;
    	beforeFirst();
    }   
	
	@Override
	public void beforeFirst() {
        s1.beforeFirst();
        s1.next();
        s2.beforeFirst();
	}

	@Override
	public boolean next() {
        boolean hasmore2 = s2.next();
        while (hasmore2) { 	
            if (s2.getVal(fldname2).equals(s1.getVal(fldname1)))
                return true;
            else {
                hasmore2 = s2.next();
	        }
       }
       
       s2.beforeFirst();
       
       boolean hasmore1 = s1.next();
       
       while (hasmore1) {
           hasmore2 = s2.next();
           while (hasmore2) {
               if (s2.getVal(fldname2).equals(s1.getVal(fldname1))) {
                   return true;
               }
               else {
            	   hasmore2 = s2.next(); 
               }
            }
           hasmore1 = s1.next();
           s2.beforeFirst();
       }
       return false;
	}

	@Override
	public int getInt(String fldname) {
	    if (s1.hasField(fldname)) {
	        return s1.getInt(fldname);
	    }
	    else {
	        return s2.getInt(fldname);
	    }
	}

	@Override
	public String getString(String fldname) {
	    if (s1.hasField(fldname)) {
	        return s1.getString(fldname);
	    }
	    else {
	        return s2.getString(fldname);
	    }
    }

	@Override
	public Constant getVal(String fldname) {
	    if (s1.hasField(fldname)) {
	        return s1.getVal(fldname);
	    }
	    else {
	        return s2.getVal(fldname);
	    }
    }

	@Override
	public boolean hasField(String fldname) {
	    return s1.hasField(fldname) || s2.hasField(fldname);
	}

	
	@Override
	public void close() {
	    s1.close();
	    s2.close();
	}

	@Override
	public double getDouble(String fldname) {
	    if (s1.hasField(fldname)) {
	        return s1.getDouble(fldname);
	    }
	    else {
	        return s2.getDouble(fldname);
	    }
	}

}