package simpledb;

import java.io.*;
import java.util.*;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */

    private TransactionId t;
    private DbIterator child;
    private DbIterator[] children = new DbIterator[1];
    private boolean called;
    private Tuple output;

    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
        this.t = t;
        this.child = child;
        this.children[0] = child;

        Type[] typeAr = new Type[1];
        typeAr[0] = Type.INT_TYPE;
        TupleDesc tupleDesc = new TupleDesc(typeAr);
        this.output = new Tuple(tupleDesc);
        this.called = false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.output.getTupleDesc();
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        this.child.open();
        this.called = false;
    }

    public void close() {
        // some code goes here
        super.close();
        this.child.close();
        this.called = false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child.rewind();
        this.called = false;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here

        if (this.called == false){
            //if we haven't called this before
            int counter = 0;
            while (this.child.hasNext()){
                Tuple next = this.child.next();
                Database.getBufferPool().deleteTuple(this.t, next);
                counter++;
            }
            this.called = true;
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
