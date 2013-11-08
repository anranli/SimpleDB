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
    int highest[];
    int lowest[];
    int ntuples;
    double range;
    double split;

    public IntHistogram(int buckets, int min, int max) {
        // some code goes here
        this.buckets = buckets;
        this.min = min;
        this.max = max;
        this.range = this.max - this.min + 1;
        this.ntuples = 0;
        createBuckets();
    }

    public void createBuckets(){
        //check to see if buckets is 0

        if (this.range < this.buckets){
            this.int_histogram = new int[(int) this.range];
            this.lowest = new int[(int) this.range];
            this.highest = new int[(int) this.range];
            this.buckets = (int) this.range;
            this.split = 1;
        }
        else {
            this.int_histogram = new int[this.buckets];
            this.lowest = new int[this.buckets];
            this.highest = new int[this.buckets];
            this.split = range/((double) buckets);
        }
    }

    public int index(int number){
        if (!(number > this.max) && !(number < this.min)){
            int slided_index = number - this.min;
            if (number == this.max){
                return (this.buckets - 1);
            }
            else {
                int correct_index = (int) Math.round(slided_index/this.split);
                if (correct_index == this.buckets){
                    return correct_index - 1;
                }
                else {
                    return correct_index;
                }
            }
        }
        else {
            //number given is too low
            return -1;
        }
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
            this.ntuples += 1;
            if (this.int_histogram[index] == 1){
                this.lowest[index] = v;
                this.highest[index] = v;
            }
            else {
                if (this.lowest[index] > v){
                    this.lowest[index] = v;
                }
                else if (this.highest[index] < v){
                    this.highest[index] = v;
                }
            }
        }
    }

    public double greaterThan(int index, int v){
        double selectivity = 0.0;
        for (int i = index; i < this.buckets; i++){
            if (this.int_histogram[i] != 0){
                double b_f = ((double) this.int_histogram[i])/(double) this.ntuples;
                double b_part = 1.0;
                //if we aren't in the index that contains v and its greater, just include it
                if (v > this.lowest[i]){
                    b_part = ((double) (this.highest[i] - v)/(double) (this.highest[i] - this.lowest[i] + 1));
                }
                selectivity += b_f * b_part;
            }
        }
        return selectivity;
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
        int index = this.index(v);
        if (op == Predicate.Op.EQUALS){
            if ((v > this.max) || (v < this.min)){
                return 0;
            }

            if (this.int_histogram[index] == 0){
                return 0;
            }
            else {
                double width = this.highest[index] - this.lowest[index] + 1;
                return (double) (((double) this.int_histogram[index])/width)/this.ntuples;
            }
        }
        else if (op == Predicate.Op.GREATER_THAN){
            if (v > this.max){
                return 0.0;
            }
            else if (v < this.min){
                return 1.0;
            }

            if (this.highest[index] > v){
                return this.greaterThan(index, v);
            }
            else {
                return this.greaterThan(index + 1, v);
            }
        }
        else if (op == Predicate.Op.GREATER_THAN_OR_EQ){
            return (this.estimateSelectivity(Predicate.Op.EQUALS, v) + this.estimateSelectivity(Predicate.Op.GREATER_THAN, v));
            
        }
        else if (op == Predicate.Op.LESS_THAN){
            return (1.0 - this.estimateSelectivity(Predicate.Op.GREATER_THAN_OR_EQ, v));
        }
        else if (op == Predicate.Op.LESS_THAN_OR_EQ){
            return (1.0 - this.estimateSelectivity(Predicate.Op.GREATER_THAN, v));
        }
        else if (op == Predicate.Op.NOT_EQUALS){
            return (this.estimateSelectivity(Predicate.Op.GREATER_THAN, v) + this.estimateSelectivity(Predicate.Op.LESS_THAN, v));
        }
        else {
            return 0.0;
        }
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
        //Piazza 742
        // some code goes here
        return -1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {

        // some code goes here
        return null;
    }
}
