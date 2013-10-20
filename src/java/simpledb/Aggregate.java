package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    DbIterator child;
    int afield;
    int gfield;
    Aggregator.Op aop;
    DbIterator iterator;
    DbIterator[] children = new DbIterator[1];
    TupleDesc desc;

    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
	// some code goes here
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;
        this.iterator = null;
        this.children[0] = child;
        
        if (gfield == Aggregator.NO_GROUPING) {
    		Type t[] = { child.getTupleDesc().getFieldType(this.afield)};
    		String[] f = {child.getTupleDesc().getFieldName(this.afield)};
    		desc = new TupleDesc(t,f);
    	}
    	else {
    		Type[] t = {child.getTupleDesc().getFieldType(gfield), child.getTupleDesc().getFieldType(this.afield)};
    		String[] f = {child.getTupleDesc().getFieldName(this.gfield), child.getTupleDesc().getFieldName(this.afield)};
    		desc = new TupleDesc(t,f);
    	}
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	// some code goes here
	   //if (this.gfield != -1){
            return this.gfield;
       //}
       //else {
       //     return simpledb.Aggregator.NO_GROUPING;
       //}
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
	// some code goes here
        if (this.gfield != Aggregator.NO_GROUPING){
            return this.child.getTupleDesc().getFieldName(this.gfield);
       }
       else {
            return null;
       }
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	// some code goes here
        return this.afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	// some code goes here
        return this.child.getTupleDesc().getFieldName(this.afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	// some code goes here
	   return this.aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	   return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
    // some code goes here
        super.open();
        
        Aggregator temp = null; 
        if (this.child.getTupleDesc().getFieldType(this.afield) == Type.STRING_TYPE){
            temp = new StringAggregator(this.groupField(), this.child.getTupleDesc().getFieldType(this.groupField()), this.aggregateField(), this.aggregateOp());
        }
        else if (this.child.getTupleDesc().getFieldType(this.afield) == Type.INT_TYPE){
            temp = new IntegerAggregator(this.groupField(), this.child.getTupleDesc().getFieldType(this.groupField()), this.aggregateField(), this.aggregateOp());
        }
        else {
            //throw an error if not one of these types
        	throw new NoSuchElementException("Only String and Int Aggregators are supported.");
        }
        child.open();
        while (child.hasNext()) {
        	temp.mergeTupleIntoGroup(child.next());
        }
        child.close();
        this.iterator = temp.iterator();
        
        this.iterator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	// some code goes here
	   if (this.iterator != null){
            if (this.iterator.hasNext()){
                return iterator.next();
            }
            else {
                return null;
            }
       }
       else {
        return null;
       }
    }

    public void rewind() throws DbException, TransactionAbortedException {
	// some code goes here
        if (this.iterator != null){
            this.iterator.rewind();
        }
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	// some code goes here
    	return desc;
    }

    public void close() {
	// some code goes here
    	if (this.iterator != null){
            this.iterator.close();
        }
    }

    @Override
    public DbIterator[] getChildren() {
	// some code goes here
	   return this.children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
	// some code goes here
        this.children[0] = children[0];
        this.child = children[0];
    }
    
}