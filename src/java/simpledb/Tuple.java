package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Create a new tuple with the specified schema (type).
     * 
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */

    private TupleDesc tupleDesc;
    private Field[] fieldAr;
    private RecordId recordID;
    private int size;

    public Tuple(TupleDesc td) {
        // some code goes here
	if (td.numFields() >= 1){
	    tupleDesc = td;
	    size = td.numFields();
	    fieldAr = new Field[td.numFields()];
	}
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return this.recordID;
    }

    /**
     * Set the RecordId information for this tuple.
     * 
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
	this.recordID = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     * 
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
	if ((i < this.size) && (0 <= i)){
	    if ((f.getType().equals(this.tupleDesc.getFieldType(i)))){
		this.fieldAr[i] = f;
	    }
	    else {
		throw new RuntimeException("Invalid field types.");
	    }
	}
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     * 
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
	if ((i < this.size) && (0 <= i)){
	    return this.fieldAr[i];
	}
	else {
	    return null;
	}
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * 
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     * 
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
        // some code goes here
	String output = "";
	for (int i = 0; i < this.size; i++){
	    output = output + this.getField(i) + "\t";
	}
	output = output + "\n";
        return output;
    }
    
    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
	class iteration implements Iterator<Field> {
	    private int currentIndex;
	    private Tuple tp;

	    public iteration(Tuple thisTuple){
		currentIndex = 0;
		tp = thisTuple;
	    }
	    
	    public boolean hasNext() {
		return currentIndex < tp.size;
	    }

	    public Field next() {
		return tp.fieldAr[currentIndex++];
	    }

            public void remove() {
		throw new UnsupportedOperationException();
	      
            }
	};
	return new iteration(this);
    
    }
}

