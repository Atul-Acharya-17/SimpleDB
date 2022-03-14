package simpledb.materialize;

import java.util.List;

import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

public class DistinctPlan implements Plan{

   private Plan p;
   private Schema sch;
   private List<String> distinctfields;
   
   public DistinctPlan(Transaction tx, Plan p, List<String> distinctfields) {
	   this.p = new SortPlan(tx, p, distinctfields);
	   this.distinctfields = distinctfields;
	   this.sch = p.schema();
   }
	   
	@Override
	public Scan open() {
		// TODO Auto-generated method stub
      Scan s = p.open();
      return new DistinctScan(s, distinctfields);
	}

	@Override
	public int blocksAccessed() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int recordsOutput() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int distinctValues(String fldname) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Schema schema() {
		// TODO Auto-generated method stub
		return sch;
	}
	
}
