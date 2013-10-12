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

    private File f;
    private TupleDesc td;
    private BufferPool bufferPool;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
	this.f = f;
	this.td = td;
	bufferPool = new BufferPool(BufferPool.DEFAULT_PAGES);
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return f;
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
	return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
	return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid){
	byte[] b = HeapPage.createEmptyPageData();
	try {
		RandomAccessFile file = new RandomAccessFile(f,"r");
		int offset = pid.pageNumber() * BufferPool.PAGE_SIZE;
		file.seek(offset);
		file.read(b);
        	return new HeapPage((HeapPageId)pid, b);
	}
	catch (Exception e) {		
	}
	return null;
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
        return (int) Math.ceil((float)(f.length()) / BufferPool.PAGE_SIZE);
    }

    // see DbFile.java for javadocs
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

    public ArrayList<Tuple> getTupleList(TransactionId tid) throws DbException, TransactionAbortedException{
	ArrayList<Tuple> list = new ArrayList<Tuple>();
	int np = numPages();
	for (int i = 0; i < np; i++) {
		HeapPageId pid = new HeapPageId(getId(), i);
		HeapPage p = (HeapPage) (bufferPool.getPage(tid, pid, Permissions.READ_ONLY));
		list.addAll(p.getTupleList());
	}
	return list;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid){
        return new HeapFileIterator(tid, this);
    }

    public class HeapFileIterator implements DbFileIterator {
	private TransactionId tid;
	private HeapFile heapFile;
	private Iterator<Tuple> iterator;
	private boolean open;

	public HeapFileIterator(TransactionId tid, HeapFile heapFile) {
		this.tid = tid;
		this.heapFile = heapFile;
		open = false;
	}

	public void open() throws DbException, TransactionAbortedException {
		if (open) {
			throw new DbException("already open");
		}
		open = true;
		iterator = heapFile.getTupleList(tid).iterator();
	}
	
	public boolean hasNext() {
		if (!open) {
			return false;
		}
		return iterator.hasNext();
	}

	public Tuple next() throws NoSuchElementException{
		if (!open) {
			throw new NoSuchElementException();
		}
		return iterator.next();
	}
	
	public void rewind() throws DbException, TransactionAbortedException {
		close();
		open();
	}

	public void close() {
		open = false;
		iterator = null;
	}
	
    }

}

