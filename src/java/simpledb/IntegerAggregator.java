package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

	private static final long serialVersionUID = 1L;
	
	int gbfield;
	Type gbfieldtype;
	int afield;
	Op what;
	TupleDesc tupleDesc;
	ArrayList<Tuple> list;
	HashMap<Field, ArrayList<Integer>> counts;

	/**
	 * Aggregate constructor
	 * 
	 * @param gbfield
	 *            the 0-based index of the group-by field in the tuple, or
	 *            NO_GROUPING if there is no grouping
	 * @param gbfieldtype
	 *            the type of the group by field (e.g., Type.INT_TYPE), or null
	 *            if there is no grouping
	 * @param afield
	 *            the 0-based index of the aggregate field in the tuple
	 * @param what
	 *            the aggregation operator
	 */
	public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
		// some code goes here
		this.gbfield = gbfield;
		this.gbfieldtype = gbfieldtype;
		this.afield = afield;
		this.what = what;

		if (gbfield != Aggregator.NO_GROUPING) {
			Type[] typeArray = {gbfieldtype, Type.INT_TYPE};
			this.tupleDesc = new TupleDesc(typeArray);
		}
		else {
			Type[] typeArray = {Type.INT_TYPE};
			this.tupleDesc = new TupleDesc(typeArray);
		}

		list = new ArrayList<Tuple>();
		counts = new HashMap<Field, ArrayList<Integer>>();
	}

	/**
	 * Merge a new tuple into the aggregate, grouping as indicated in the
	 * constructor
	 * 
	 * @param tup
	 *            the Tuple containing an aggregate field and a group-by field
	 */
	public void mergeTupleIntoGroup(Tuple tup) {
		// some code goes here
		if (!tupleDesc.equals(tup.getTupleDesc())){
			System.out.println("TupleDesc does not match");
			return; //TODO throw error?
		}

		if (gbfield == Aggregator.NO_GROUPING){
			if (list.size() == 0) {
				IntField f = (IntField) tup.getField(afield);
				
				ArrayList<Integer> values = new ArrayList<Integer>();
				values.add(f.getValue());
				counts.put(new IntField(Aggregator.NO_GROUPING), values);
				
				if (what == Op.COUNT) {
					tup.setField(afield, new IntField(1));
				}
				list.add(tup);
				return;
			}
			switch (what) {
			case MIN:  minMerge(tup);
			break;
			case MAX:  maxMerge(tup);
			break;
			case SUM:  sumMerge(tup);
			break;
			case AVG:  avgMerge(tup);
			break;
			case COUNT:  countMerge(tup);
			break;
			}
		}
		else {
			if (list.size() == 0) {
				Field f1 = tup.getField(gbfield);
				IntField f2 = (IntField) tup.getField(afield);
				
				ArrayList<Integer> values = new ArrayList<Integer>();
				values.add(f2.getValue());
				counts.put(f1, values);
				
				if (what == Op.COUNT) {
					tup.setField(afield, new IntField(1));
				}
				list.add(tup);
				return;
			}
			switch (what) {
			case MIN:  minGroupMerge(tup);
			break;
			case MAX:  maxGroupMerge(tup);
			break;
			case SUM:  sumGroupMerge(tup);
			break;
			case AVG:  avgGroupMerge(tup);
			break;
			case COUNT:  countGroupMerge(tup);
			break;
			}
		}
	}

	public void minMerge(Tuple tup){
		boolean comp = list.get(0).getField(afield).compare(Predicate.Op.GREATER_THAN, tup.getField(afield));
		if (comp) {
			list.set(0, tup);
		}
	}

	public void maxMerge(Tuple tup){
		boolean comp = list.get(0).getField(afield).compare(Predicate.Op.LESS_THAN, tup.getField(afield));
		if (comp) {
			list.set(0, tup);
		}
	}

	public void sumMerge(Tuple tup){
		IntField f1 = (IntField)(list.get(0).getField(afield));
		IntField f2 =(IntField)(tup.getField(afield));
		Field f = new IntField(f1.getValue() + f2.getValue());
		list.get(0).setField(afield, f);
	}

	public void avgMerge(Tuple tup){
		ArrayList<Integer> values = counts.get(new IntField(Aggregator.NO_GROUPING));
		
		IntField fTemp = (IntField) tup.getField(afield);
		values.add(fTemp.getValue());
		
		int sum = 0;
		for (int j = 0; j < values.size(); j++){
			sum += values.get(j);
		}
		int calculation = sum/values.size();
		
		IntField f = new IntField(calculation);
		list.get(0).setField(afield, f);
	}

	public void countMerge(Tuple tup){
		ArrayList<Integer> values = counts.get(new IntField(Aggregator.NO_GROUPING));
		
		IntField fTemp = (IntField) tup.getField(afield);
		values.add(fTemp.getValue());
		
		IntField f = new IntField(values.size());
		list.get(0).setField(afield, f);
	}
	
	public void minGroupMerge(Tuple tup){
		for (int i = 0; i < list.size(); i++){
			if (list.get(i).getField(gbfield).equals(tup.getField(gbfield))){
				boolean comp = list.get(i).getField(afield).compare(Predicate.Op.GREATER_THAN, tup.getField(afield));
				if (comp) {
					list.set(i, tup);
				}
				return;
			}
		}
		list.add(tup);
	}

	public void maxGroupMerge(Tuple tup){
		for (int i = 0; i < list.size(); i++){
			if (list.get(i).getField(gbfield).equals(tup.getField(gbfield))){
				boolean comp = list.get(i).getField(afield).compare(Predicate.Op.LESS_THAN, tup.getField(afield));
				if (comp) {
					list.set(i, tup);
				}
				return;
			}
		}
		list.add(tup);
	}

	public void sumGroupMerge(Tuple tup){
		for (int i = 0; i < list.size(); i++){
			if (list.get(i).getField(gbfield).equals(tup.getField(gbfield))){
				IntField f1 = (IntField)(list.get(i).getField(afield));
				IntField f2 =(IntField)(tup.getField(afield));
				Field f = new IntField(f1.getValue() + f2.getValue());
				list.get(i).setField(afield, f);
				return;
			}
		}
		list.add(tup);
	}

	public void avgGroupMerge(Tuple tup){
		for (int i = 0; i < list.size(); i++){
			if (list.get(i).getField(gbfield).equals(tup.getField(gbfield))){
				Field f1 = list.get(i).getField(gbfield);
				ArrayList<Integer> values = counts.get(f1);
				
				IntField fTemp = (IntField) tup.getField(afield);
				values.add(fTemp.getValue());
				
				int sum = 0;
				for (int j = 0; j < values.size(); j++){
					sum += values.get(j);
				}
				int calculation = sum/values.size();
				
				IntField f2 = new IntField(calculation);
				list.get(i).setField(afield, f2);
				return;
			}
		}
		list.add(tup);
		
		Field f1 = tup.getField(gbfield);
		IntField f2 = (IntField) tup.getField(afield);
		
		ArrayList<Integer> values = new ArrayList<Integer>();
		values.add(f2.getValue());
		counts.put(f1, values);
	}

	public void countGroupMerge(Tuple tup){
		for (int i = 0; i < list.size(); i++){
			if (list.get(i).getField(gbfield).equals(tup.getField(gbfield))){
				Field f1 = list.get(i).getField(gbfield);
				ArrayList<Integer> values = counts.get(f1);
				
				IntField fTemp = (IntField) tup.getField(afield);
				values.add(fTemp.getValue());
				
				IntField f2 = new IntField(values.size());
				list.get(i).setField(afield, f2);
				return;
			}
		}
		Field f1 = tup.getField(gbfield);
		IntField f2 = (IntField) tup.getField(afield);
		
		ArrayList<Integer> values = new ArrayList<Integer>();
		values.add(f2.getValue());
		counts.put(f1, values);
		
		tup.setField(afield, new IntField(1));
		list.add(tup);
	}

	/**
	 * Create a DbIterator over group aggregate results.
	 * 
	 * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
	 *         if using group, or a single (aggregateVal) if no grouping. The
	 *         aggregateVal is determined by the type of aggregate specified in
	 *         the constructor.
	 */
	public DbIterator iterator() {
		// some code goes here
		return new TupleIterator(tupleDesc, list);
	}

}