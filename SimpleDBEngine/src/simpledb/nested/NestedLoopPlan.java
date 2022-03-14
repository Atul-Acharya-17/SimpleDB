package simpledb.nested;


import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

public class NestedLoopPlan implements Plan {
	private Plan p1, p2;
	private String fldname1, fldname2;
	private Schema sch = new Schema();
	   
	public NestedLoopPlan(Transaction tx, Plan p1, Plan p2, String fldname1, String fldname2) {
		this.fldname1 = fldname1;
	    this.p1 = p1;
	    this.fldname2 = fldname2;
	    this.p2 = p2;
	    sch.addAll(p1.schema());
		sch.addAll(p2.schema());
	 }
	
	@Override
	public Scan open() {
		Scan s1 = p1.open();
		Scan s2 = p2.open();
		return new NestedLoopScan(s1, s2, fldname1, fldname2);
	}

	@Override
	public int blocksAccessed() {
		return p1.blocksAccessed() + p1.recordsOutput() * p2.blocksAccessed();
	}

	@Override
	public int recordsOutput() {
		int maxvals = Math.max(p1.distinctValues(fldname1),p2.distinctValues(fldname2));
		return (p1.recordsOutput() * p2.recordsOutput()) / maxvals;
	}

	@Override
	public int distinctValues(String fldname) {
		if (p1.schema().hasField(fldname)) {
			return p1.distinctValues(fldname);
		}
	    else {
	        return p2.distinctValues(fldname);
	    }
	}

	@Override
	public Schema schema() {
		return sch;
	}

}