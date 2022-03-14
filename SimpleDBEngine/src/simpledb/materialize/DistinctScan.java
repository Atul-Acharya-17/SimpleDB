package simpledb.materialize;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

import simpledb.query.Constant;
import simpledb.query.Scan;


public class DistinctScan implements Scan{
	
	private Scan s;
	private List<String> distinctfields;
	private boolean hashmapready = false;
	private HashMap<String, Constant> uniquetuples;
	
	public DistinctScan(Scan s, List<String> distinctfields) {
		this.s = s;
		this.distinctfields = distinctfields;
		uniquetuples = new LinkedHashMap<String, Constant>();
		for (String field: distinctfields) {
			uniquetuples.put(field, new Constant(0));
		}
		beforeFirst();
	}

	@Override
	public void beforeFirst() {
		s.beforeFirst();
		
	}

	@Override
	public boolean next() {
		boolean hasmore = s.next();
		if (hasmore && !hashmapready) {
			hashmapready = true;
			for (String key: distinctfields) 
				uniquetuples.replace(key, s.getVal(key));
			return true;
		}
		
		else if (hasmore) {
			while (hasmore) {
				HashMap<String, Constant> newtuples;
				newtuples = new LinkedHashMap<String, Constant>();
				for (String key: distinctfields) 
					newtuples.put(key, s.getVal(key));
				if (isSame(newtuples))
					hasmore = s.next();
				
				else {
					uniquetuples = newtuples;
					return true;
				}
			}
		}
		return false;
	}

	@Override
	public int getInt(String fldname) {
	      if (hasField(fldname))
	          return s.getInt(fldname);
	       else
	          throw new RuntimeException("field " + fldname + " not found.");
	}

	@Override
	public String getString(String fldname) {
	      if (hasField(fldname))
	          return s.getString(fldname);
	       else
	          throw new RuntimeException("field " + fldname + " not found.");
	}

	public double getDouble(String fldname) {
	      if (hasField(fldname))
	          return s.getDouble(fldname);
	       else
	          throw new RuntimeException("field " + fldname + " not found.");
	}
	
	@Override
	public Constant getVal(String fldname) {
	      if (hasField(fldname))
	          return s.getVal(fldname);
	       else
	          throw new RuntimeException("field " + fldname + " not found.");
	}

	@Override
	public boolean hasField(String fldname) {
	      if (s.hasField(fldname))
	    	  return true;
	      return false;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		s.close();
	}
	
	public boolean isSame(HashMap<String, Constant> newtuple) {
		List<String> keys = new ArrayList<String>(uniquetuples.keySet());
		
		Collections.reverse(keys);
		for (String key: keys) {
			Constant current = uniquetuples.get(key);
			Constant newtup = newtuple.get(key);
			if (current.compareTo(newtup) != 0)
				return false;
		}
		
		return true;
	}

}
