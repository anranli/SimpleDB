package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing proj1 and proj2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */

    HeapFile file;
    DbFileIterator iterator;
    int tableid;
    int ioCostPerPage;
    int min[];
    int max[];
    Object histograms[];
    double numTuples;

    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here

        this.numTuples = 0.0;
        this.tableid = tableid;
        this.ioCostPerPage = ioCostPerPage;
        this.file = (HeapFile) Database.getCatalog().getDbFile(tableid);
        try {
            this.scanAndFile();
        }
        catch (Exception e){
            System.out.println("fuck");
        }
    }

    public void scanAndFile() throws DbException, TransactionAbortedException {
        TransactionId tid = new TransactionId();

        //first scan to find the min max of all fields
        this.iterator = file.iterator(tid);
        this.iterator.open();
        this.fieldRanges();
        this.createHistograms();
        
        //second scan to insert records after creating the histograms
        this.iterator.rewind();
        while (this.iterator.hasNext()){

            Tuple tuple = this.iterator.next();
            for (int i = 0; i < this.file.getTupleDesc().numFields(); i++){
                if (this.file.getTupleDesc().getFieldType(i).equals(Type.INT_TYPE)){
                    ((IntHistogram) this.histograms[i]).addValue(((IntField) tuple.getField(i)).getValue());
                }
                else if (this.file.getTupleDesc().getFieldType(i).equals(Type.STRING_TYPE)){
                    ((StringHistogram) this.histograms[i]).addValue(((StringField) tuple.getField(i)).getValue());
                }
            }
            this.numTuples += 1.0;
        }
        this.iterator.close();
    }

    public void fieldRanges() throws DbException, TransactionAbortedException{
        //go through all the tuples
        while (this.iterator.hasNext()){
            Tuple tuple = this.iterator.next();
            //if we haven't initialized min or max
            if (this.min == null){
                this.min = new int[this.file.getTupleDesc().numFields()];
                this.max = new int[this.file.getTupleDesc().numFields()];
                //go through each field in the tuple and set the value to be this first tuple
                for (int i = 0; i < this.file.getTupleDesc().numFields(); i++){
                    Field field = tuple.getField(i);
                    if (field.getType().equals(Type.INT_TYPE)){
                        this.min[i] = ((IntField) field).getValue();
                        this.max[i] = ((IntField) field).getValue();
                    }
                    //not so sure what to do for string
                }
            }
            //if we have initialized min or max, then it has a value
            else {
                //go through each field in the tuple and check for min or max
                for (int i = 0; i < this.file.getTupleDesc().numFields(); i++){
                    Field field = tuple.getField(i);
                    if (field.getType().equals(Type.INT_TYPE)){
                        int value = ((IntField) field).getValue();
                        if (this.min[i] > value){
                            this.min[i] = value;
                        }
                        else if (this.max[i] < value){
                            this.max[i] = value;
                        }
                    }
                }
            }
        }
    }

    public void createHistograms(){
        this.histograms = new Object[this.file.getTupleDesc().numFields()];
        for (int i = 0; i < this.file.getTupleDesc().numFields(); i++){
            if (this.file.getTupleDesc().getFieldType(i).equals(Type.INT_TYPE)){
                this.histograms[i] = new IntHistogram(NUM_HIST_BINS, this.min[i], this.max[i]);
            }
            else if (this.file.getTupleDesc().getFieldType(i).equals(Type.STRING_TYPE)){
                this.histograms[i] = new StringHistogram(NUM_HIST_BINS);
            }
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return (double) (this.file.numPages() * this.ioCostPerPage);
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here

        //using numTuples may not be correct
        // System.out.println(selectivityFactor);
        return (int) Math.floor(this.numTuples*selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        if (constant.getType().equals(Type.INT_TYPE)){
            IntHistogram histogram = (IntHistogram) this.histograms[field];
            return histogram.estimateSelectivity(op, ((IntField) constant).getValue());
        }
        else if (constant.getType().equals(Type.STRING_TYPE)){
            StringHistogram histogram = (StringHistogram) this.histograms[field];
            return histogram.estimateSelectivity(op, ((StringField) constant).getValue());
        }
        else {
            return -1.0;
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return (int) this.numTuples;
    }

}
