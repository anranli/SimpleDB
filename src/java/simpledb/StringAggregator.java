package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

import simpledb.Aggregator.Op;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    
	int gbfield;
	Type gbfieldtype;
	int afield;
	Op what;
	TupleDesc tupleDesc;
	ArrayList<Tuple> list;
	HashMap<Field, ArrayList<String>> counts;
	TupleDesc sample;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	this.gbfield = gbfield;
		this.gbfieldtype = gbfieldtype;
		this.afield = afield;
		this.what = what;
		
		if (what != Aggregator.Op.COUNT) {
			throw new IllegalArgumentException("StringAggregator can only use the COUNT Op");
		}

		if (gbfield != Aggregator.NO_GROUPING) {
			Type[] typeArray = {gbfieldtype, Type.STRING_TYPE};
			this.tupleDesc = new TupleDesc(typeArray);
			
			Type[] ta = {gbfieldtype, Type.INT_TYPE};
			sample = new TupleDesc(ta);
		}
		else {
			Type[] typeArray = {Type.STRING_TYPE};
			this.tupleDesc = new TupleDesc(typeArray);
			
			Type[] ta = {Type.INT_TYPE};
			sample = new TupleDesc(ta);
		}

		list = new ArrayList<Tuple>();
		counts = new HashMap<Field, ArrayList<String>>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	if (!tupleDesc.equals(tup.getTupleDesc())){
			return; //TODO throw error?
		}

		if (gbfield == Aggregator.NO_GROUPING){
			if (list.size() == 0) {
				IntField intField = new IntField(1);
				Tuple toAdd = new Tuple(sample);
				toAdd.setField(afield, intField);
				list.add(toAdd);
				
				StringField f = (StringField) tup.getField(afield);
				ArrayList<String> values = new ArrayList<String>();
				values.add(f.getValue());
				counts.put(new IntField(Aggregator.NO_GROUPING), values);
				return;
			}
			countMerge(tup);
		}
		else {
			if (list.size() == 0) {
				IntField intField = new IntField(1);
				Tuple toAdd = new Tuple(sample);
				toAdd.setField(gbfield, tup.getField(gbfield));
				toAdd.setField(afield, intField);
				list.add(toAdd);
				
				Field f1 = tup.getField(gbfield);
				StringField f2 = (StringField) tup.getField(afield);
				
				ArrayList<String> values = new ArrayList<String>();
				values.add(f2.getValue());
				counts.put(f1, values);
				return;
			}
			countGroupMerge(tup);
		}
    }
    
    public void countMerge(Tuple tup){
		ArrayList<String> values = counts.get(Aggregator.NO_GROUPING);
		
		StringField fTemp = (StringField) tup.getField(afield);
		values.add(fTemp.getValue());
		
		IntField f = new IntField(values.size());
		list.get(0).setField(afield, f);
	}
    
    public void countGroupMerge(Tuple tup){
		for (int i = 0; i < list.size(); i++){
			if (list.get(i).getField(gbfield).equals(tup.getField(gbfield))){
				Field f1 = list.get(i).getField(gbfield);
				ArrayList<String> values = counts.get(f1);
				
				StringField fTemp = (StringField) tup.getField(afield);
				values.add(fTemp.getValue());
				
				IntField f2 = new IntField(values.size());
				list.get(i).setField(afield, f2);
				return;
			}
		}
		IntField intField = new IntField(1);
		Tuple toAdd = new Tuple(sample);
		toAdd.setField(0, tup.getField(0));
		toAdd.setField(1, intField);
		list.add(toAdd);
		
		Field f1 = tup.getField(gbfield);
		StringField f2 = (StringField) tup.getField(afield);
		
		ArrayList<String> values = new ArrayList<String>();
		values.add(f2.getValue());
		counts.put(f1, values);
	}

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
    	return new TupleIterator(sample, list);
    }

}
