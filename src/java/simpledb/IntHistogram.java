package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */

    int buckets;
    int min;
    int max;
    int int_histogram[];
    double range;
    double split;

    public IntHistogram(int buckets, int min, int max) {
        // some code goes here
        this.buckets = buckets;
        this.min = min;
        this.max = max;
        this.range = this.max - this.min + 1;
        createBuckets();
    }

    public void createBuckets(){
        //check to see if buckets is 0

        if (this.range < this.buckets){
            this.int_histogram = new int[(int) this.range];
            this.buckets = (int) this.range;
            this.split = 1;
        }
        else {
            this.int_histogram = new int[this.buckets];
            this.split = range/buckets;
        }
    }

    public int index(int number){
        int slided_index = number - this.min;
        if (number == this.max){
            return (this.buckets - 1);
        }
        else {
            int correct_index = (int) Math.round(slided_index/this.split);
            return correct_index;
        }
    } 

    public void getWidthOfBucket(int index){
        
    }

    /** 
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // some code goes here
        if (!(v > this.max) && !(v < this.min)){
            int index = this.index(v);
            this.int_histogram[index] += 1;
        }
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

        // some code goes here
        System.out.println(op);
        if (op.equals(EQUALS)){

        }
        return -1.0;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {

        // some code goes here
        return null;
    }
}
