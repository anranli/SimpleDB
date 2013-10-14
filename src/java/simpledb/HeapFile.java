package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    private File file;
    private TupleDesc tupleDesc;

    public HeapFile(File f, TupleDesc td) {
        // some code goes here
	file = f;
	tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
	HeapPage pg = null;
	try {
	    byte[] byteArray = new byte[BufferPool.PAGE_SIZE];
	    RandomAccessFile randAccessFile = new RandomAccessFile(this.file, "r");
	    randAccessFile.skipBytes(pid.pageNumber()*BufferPool.PAGE_SIZE);
	    randAccessFile.readFully(byteArray);
	    randAccessFile.close();
	    pg = new HeapPage((HeapPageId) pid, byteArray);
	}
	catch(Exception e){
        }
	return pg;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
	
        return (int) (this.file.length())/(BufferPool.PAGE_SIZE);
    }

    // see DbFile.java for javadocs
    /**
    * @param tid The transaction performing the update
    * @param t The tuple to add.  This tuple should be updated to reflect that
    *          it is now stored in this file.
    * @return An ArrayList contain the pages that were modified
    * @throws DbException if the tuple cannot be added
    * @throws IOException if the needed file can't be read/written
    **/
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here

        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
	class iteration implements DbFileIterator{

	    private int currentIndex;
	    private HeapFile fl;
	    private TransactionId transId;
	    private Iterator<Tuple> iteration;
	    
	    public iteration(HeapFile thisFile, TransactionId tid){
		currentIndex = 0;
		fl = thisFile;
		transId = tid;
		iteration = null;
	    }
	    
	    public void open() throws TransactionAbortedException, DbException{
		HeapPageId hpgId = new HeapPageId(fl.getId(), this.currentIndex);
		HeapPage hpg = ((HeapPage) (Database.getBufferPool()).getPage(this.transId, hpgId, Permissions.READ_ONLY));
		iteration = hpg.iterator();
	    }
	    
	    public boolean hasNext() throws TransactionAbortedException, DbException {
		if (this.iteration != null){
		    if (this.iteration.hasNext()){
			return true;
		    }
		    else if (this.currentIndex < (fl.numPages() - 1)){
			currentIndex++;
			this.open();
			return hasNext();
		    }
		    else {
			return false;
		    }
		}
		else {
		    if (this.currentIndex < (fl.numPages() - 1)){
			currentIndex++;
			this.open();
			return hasNext();  
		    }
		    else {
			return false;
		    }
		}
	    }
	

	    public Tuple next() throws TransactionAbortedException, DbException{
		if (this.iteration == null){
		    throw new NoSuchElementException();
		}
		else {
		    Tuple temp = null;
		    if (this.hasNext()){
			temp = iteration.next(); 
		    }
		    return temp;
		}
	    }
	    
	    public void close(){
		iteration = null;
	    }

	    public void rewind() throws TransactionAbortedException, DbException{
		currentIndex = 0;
		this.open();
	    }


            public void remove() {
		throw new UnsupportedOperationException();
            }
	};

	return new iteration(this, tid);
    }

}

