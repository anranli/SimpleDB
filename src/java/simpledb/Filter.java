package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */

    Predicate p;
    DbIterator child;
    DbIterator[] children = new DbIterator[1];
    public Filter(Predicate p, DbIterator child) {
        // some code goes here
        this.p = p;
        this.child = child;
        this.children[0] = child;
    }

    public Predicate getPredicate() {
        // some code goes here
        return this.p;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        this.child.open();
    }

    public void close() {
        // some code goes here
        super.close();
        this.child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        while (this.child.hasNext()){
            Tuple tuple = this.child.next();
            if (this.getPredicate().filter(tuple)){
                return tuple;
            }
        }
        return null;  
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
