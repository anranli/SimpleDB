package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor. Accepts to (two?) children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    JoinPredicate p;
    DbIterator child1;
    DbIterator child2;
    DbIterator[] children = new DbIterator[2];
    TupleIterator iterator;
    public Join(JoinPredicate p, DbIterator child1, DbIterator child2) {
        // some code goes here
        this.p = p;
        this.child1 = child1;
        this.child2 = child2;
        this.children[0] = child1;
        this.children[1] = child2;
        this.iterator = null;      
    }

    public TupleIterator tupleIterator() throws TransactionAbortedException, DbException {
        ArrayList<Tuple> tuplist = new ArrayList<Tuple>();
        this.child1.open();
        this.child2.open();
        while (this.child1.hasNext()){
            Tuple tuple1 = this.child1.next();
            while (this.child2.hasNext()){
                Tuple tuple2 = this.child2.next();

                if (this.getJoinPredicate().filter(tuple1, tuple2)){
                    Tuple tupleFinal = new Tuple(this.getTupleDesc());
                    Iterator fields = tuple1.fields();
                    int i = 0;
                    while (fields.hasNext()){
                        tupleFinal.setField(i, (Field) fields.next());
                        i++;
                    }
                    fields = tuple2.fields();
                    while (fields.hasNext()){
                        tupleFinal.setField(i, (Field) fields.next());
                        i++;
                    }
                    tuplist.add(tupleFinal);
                }
            }
            this.child2.rewind();
        }
        this.child1.close();
        this.child2.close();

        return new TupleIterator(this.getTupleDesc(), tuplist);
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return this.p;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
        return this.child1.getTupleDesc().getFieldName(this.p.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        return this.child2.getTupleDesc().getFieldName(this.p.getField2());
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return simpledb.TupleDesc.merge(this.child1.getTupleDesc(), this.child2.getTupleDesc());
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        super.open();
        if (this.iterator == null){
            this.iterator = this.tupleIterator();
            this.iterator.open();
        }
        // some code goes here
    }

    public void close() {
        super.close();
        if (this.iterator == null){
            this.iterator.close();
        }
        // some code goes here
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        if (this.iterator != null){
            this.iterator.rewind();
        }
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (this.iterator != null){
            if (this.iterator.hasNext()){
                return this.iterator.next();
            }
            else {
                return null;
            }
        }
        else {
            return null;
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
        this.children[1] = children[1];
    }

}
