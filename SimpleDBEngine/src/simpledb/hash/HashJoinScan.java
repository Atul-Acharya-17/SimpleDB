package simpledb.hash;

import java.util.List;

import simpledb.materialize.TempTable;
import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;

public class HashJoinScan implements Scan {
	private Scan s;
	List<TempTable> tables;

	public HashJoinScan(List<TempTable> tables) {
		if (!tables.isEmpty()) {
			s = tables.get(0).open();	
		}
		this.tables = tables;	
	}
	
	@Override
	public void beforeFirst() {
		s.beforeFirst();
	}

	@Override
	public boolean next() {
		while (!tables.isEmpty()) {
			boolean hasNext = s.next();
			if (hasNext) {
				return true;
			}
			if (tables.size() == 1) {
				break;
			}
			else {
				s.close();
				tables.remove(0);
				s = tables.get(0).open();
			}
		}
		return false;
	}

	@Override
	public int getInt(String fldname) {
		return s.getInt(fldname);
	}

	@Override
	public String getString(String fldname) {
		return s.getString(fldname);
	}

	@Override
	public Constant getVal(String fldname) {
		return s.getVal(fldname);
	}

	@Override
	public boolean hasField(String fldname) {
		return s.hasField(fldname);
	}

	@Override
	public void close() {
		if (!tables.isEmpty()) {
			s.close();
		}
	}

	@Override
	public double getDouble(String fldname) {
		return s.getDouble(fldname);
	}

}