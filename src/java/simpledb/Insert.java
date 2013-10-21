package simpledb;

import java.io.*;
import java.util.*;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */

    private TransactionId t;
    private DbIterator child;
    private int tableid;
    private DbIterator[] children = new DbIterator[1];
    private int called_times;
    private Tuple output;

    public Insert(TransactionId t, DbIterator child, int tableid)
            throws DbException {
        // some code goes here
        if ((Database.getCatalog()).getTupleDesc(tableid).equals(child.getTupleDesc())){ 
            this.t = t;
            this.child = child;
            this.tableid = tableid;
            this.children[0] = child;
            this.called_times = 0;

            //TupleDesc for some reason is based on output tuples, so have to declare tuple up here instead in fetchNext()
            //This would have been real nice if they explained this or pointed towards Operator.java
            Type[] typeAr = new Type[1];
            typeAr[0] = Type.INT_TYPE;
            TupleDesc tupleDesc = new TupleDesc(typeAr);
            this.output = new Tuple(tupleDesc);
        }
        else {
            throw new DbException("TupleDesc of child differs from table into which we are to insert.");
        }
    }

    /**
     * @return return the TupleDesc of the output tuples of this operator
     * */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.output.getTupleDesc();
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        this.child.open();
        this.called_times = 0;
    }

    public void close() {
        // some code goes here
        super.close();
        this.child.close();
        //perhaps this is wrong
        this.called_times = 0;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child.rewind();
        this.called_times = 0;
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {

        // some code goes here
        if (this.called_times == 0){
            //if we haven't called this before
            int counter = 0;
            while (this.child.hasNext()){
                try {
                    //if an exception happens, should we continue with the counter or stop?
                    TransactionId tid = new TransactionId();
                    Tuple next = this.child.next();
                    Database.getBufferPool().insertTuple(tid, this.tableid, next);
                    counter++;
                }
                catch (IOException io){
                    throw new DbException("IOException");
                }

            }
            called_times++;
            IntField converted_counter = new IntField(counter);
            this.output.setField(0, converted_counter);
            return output;
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
        this.child = children[0];
    }
}
