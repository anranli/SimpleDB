package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     * 
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    private TransactionId transId;
    private int tableId;
    private String tableAl;
    private HeapFile fl;
    private DbFileIterator iter;

    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
	transId = tid;
	tableId = tableid;
	tableAl = tableAlias;
	fl = (HeapFile) Database.getCatalog().getDbFile(tableId);
	iter = fl.iterator(this.transId);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
	return Database.getCatalog().getTableName(this.tableId);
        //return null;
    }
    
    /**
     * @return Return the alias of the table this operator scans. 
     * */
    public String getAlias()
    {
        // some code goes here
        return this.tableAl;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
	this.tableId = tableid;
	this.tableAl = tableAlias;
	this.fl = (HeapFile) Database.getCatalog().getDbFile(tableid);
	this.iter = fl.iterator(this.transId);
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
	this.iter.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     * 
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        TupleDesc tupleDesc = this.fl.getTupleDesc();
	String[] fieldAr = new String[tupleDesc.numFields()];
	Type[] typeAr = new Type[tupleDesc.numFields()];
	for (int i = 0; i < tupleDesc.numFields(); i++){
	    fieldAr[i] = this.getAlias() + "." + tupleDesc.getFieldName(i);
	    typeAr[i] = tupleDesc.getFieldType(i);
	}
	TupleDesc aliasTupleDesc = new TupleDesc(typeAr, fieldAr);
	return aliasTupleDesc;
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        return this.iter.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        return this.iter.next();
    }

    public void close() {
        // some code goes here
	this.iter.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
	this.iter.rewind();
    }
}
