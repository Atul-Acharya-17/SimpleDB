package simpledb.hash;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import simpledb.materialize.TempTable;
import simpledb.plan.Plan;
import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

public class HashJoinPlan implements Plan {
	private Plan p1, p2;
	private String fldname1, fldname2;
	private Schema sch = new Schema();
	public static int NUM_BUCKETS = 100;
	private Transaction tx;

	public HashJoinPlan(Transaction tx, Plan p1, Plan p2, String fldname1, String fldname2) {
		this.fldname1 = fldname1;
		this.fldname2 = fldname2;
		this.p1 = p1;
		this.p2 = p2;
		this.tx = tx;
		sch.addAll(p1.schema());
		sch.addAll(p2.schema());		
	}
	
	@Override
	public Scan open() {
		Scan s1 = p1.open();
		List<TempTable> p1partitions = splitIntoPartitions(s1, fldname1, p1.schema());
		s1.close();
		
		Scan s2 = p2.open();
		List<TempTable> p2partitions = splitIntoPartitions(s2, fldname2, p2.schema());
		s2.close();
		
		List<TempTable> mergedpartitions = new ArrayList<>();
		
		if (p1partitions.size() != 0 && p2partitions.size() != 0) {
			mergedpartitions = mergePartitions(p1partitions, p1.schema(), fldname1, p2partitions, p2.schema(), fldname2);
		}
		
		
		return new HashJoinScan(mergedpartitions);
	}
	
	
	private List<TempTable> mergePartitions(List<TempTable> partition1, Schema sch1, String fldname1, List<TempTable> partition2, Schema sch2, String fldname2){
		List<TempTable> result = new ArrayList<>();
		
		for (int i = 0; i < 100; i++) {
			result.add(mergeTwoPartitions(partition1.get(i), sch1, fldname1, partition2.get(i), sch2, fldname2));
		}
		
		return result;
	}
	
	
	private List<TempTable> splitIntoPartitions(Scan src, String fldname, Schema schema) {
		List<TempTable> partitions = new ArrayList<>();
		src.beforeFirst();
		
		if (!src.next()) {
			return partitions;
		}
		else {
			for (int i = 0; i < NUM_BUCKETS; i++) {
				partitions.add(new TempTable(tx, schema));
			}
		}
		
		Constant tuple = src.getVal(fldname);
		int bucket_number = tuple.hashCode() % NUM_BUCKETS;
		UpdateScan dest = partitions.get(bucket_number).open();
		
		while (copy(schema, src, dest)) {
			dest.close();
			tuple = src.getVal(fldname);
			bucket_number = tuple.hashCode() % NUM_BUCKETS;
			dest = partitions.get(bucket_number).open();
		}
		
		dest.close();
		return partitions;
	}
	
	private TempTable mergeTwoPartitions(TempTable t1, Schema s1, String field1, TempTable t2, Schema s2, String field2) {
		Scan src1 = t1.open();
		Scan src2 = t2.open();
		TempTable result = new TempTable(tx, sch);
		UpdateScan dest = result.open();
		
		boolean hasmore1 = src1.next();
		boolean hasmore2 = src2.next();
		
		HashMap<Integer, List<HashMap<String, Constant>>> table = new HashMap<>();
		int second_num_buckets =  50;
		for (int i = 0; i < second_num_buckets; i++) {
			table.put(i, new ArrayList<HashMap<String, Constant>>());
		}
		
		while (hasmore2) {
			int bucket_num = src2.getVal(field2).hashCode() % second_num_buckets;
			HashMap<String, Constant> map = new HashMap<>();
			for (String s: s2.fields()) {
				map.put(s, src2.getVal(s));
			}
			
			table.get(bucket_num).add(map);
			hasmore2 = src2.next();
		}
		
		
		
		while (hasmore1) {
			int bucket_num = src1.getVal(field1).hashCode() % second_num_buckets;
			List<HashMap<String, Constant>> matching_tuples = table.get(bucket_num);
			for (HashMap<String, Constant> tuple : matching_tuples) {
				if (tuple.get(field2).equals(src1.getVal(field1))) {
					dest.insert();
					for (String field : sch.fields()) {
						if (s1.hasField(field)) {
							dest.setVal(field, src1.getVal(field));
						}
						else {
							dest.setVal(field, tuple.get(field));
						}
					}
				}
			}
			hasmore1 = src1.next();
		}
		
		src1.close();
		src2.close();
		dest.close();
		return result;
	}
	
	
    private boolean copy(Schema schema, Scan src, UpdateScan dest) {
        dest.insert();
        for (String fldname : schema.fields())
            dest.setVal(fldname, src.getVal(fldname));
        return src.next();
    }

	@Override
	public int blocksAccessed() {		
		return 3 * (p1.blocksAccessed() + p2.blocksAccessed());
	}

	@Override
	public int recordsOutput() {
		int maxvals = Math.max(p1.distinctValues(fldname1),p2.distinctValues(fldname2));
		return (p1.recordsOutput() * p2.recordsOutput())/maxvals;
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