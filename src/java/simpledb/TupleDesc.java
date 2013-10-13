package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
    private ArrayList<TDItem> description;

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        Type fieldType;
        
        /**
         * The name of the field
         * */
        String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
	return this.description.iterator();
	
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */

    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        //We can assume that typeAr and fieldAr are the same length, so fieldAr can't be null

        description = new ArrayList<TDItem>();
	for (int i = 0; i < typeAr.length; i++){
	    description.add(new TDItem(typeAr[i], fieldAr[i]));
	}
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        //Anonymous = ""

	description = new ArrayList<TDItem>();
	for (int i = 0; i < typeAr.length; i++){
	    description.add(new TDItem(typeAr[i], ""));
	}
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
	return this.description.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
	if ((i >= this.numFields()) || (i < 0)){
	    throw new NoSuchElementException();
	}
	else {
	    return this.description.get(i).fieldName;
	}
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
	if ((i >= this.numFields()) || (i < 0)){
	    throw new NoSuchElementException();
	}
	return this.description.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
	int i = 0;
	while (i < this.numFields()){
	    if (this.getFieldName(i) == null){
		if (name == null){
		    throw new NoSuchElementException();
		}
		else {
		    i++;
		}
	    }
	    else if (this.getFieldName(i).equals(name)) {
		return i;
	    }
	    else {
		i++;
	    }
	}
	
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
	int size = 0;
	Iterator items = this.iterator();
	while (items.hasNext()){
	    size = size + ((TDItem) items.next()).fieldType.getLen();
	}
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here

	//improve code

	Type[] typeAr = new Type[td1.numFields() + td2.numFields()];
	String[] nameAr = new String[td1.numFields() + td2.numFields()];
	
	int i = 0;
	Iterator t1 = td1.iterator();
	Iterator t2 = td2.iterator();
	while (t1.hasNext()){
	    TDItem item = (TDItem) t1.next();
	    typeAr[i] = item.fieldType;
	    nameAr[i] = item.fieldName;
	    i++;
	}

	while (t2.hasNext()){
	    TDItem item = (TDItem) t2.next();
	    typeAr[i] = item.fieldType;
	    nameAr[i] = item.fieldName;
	    i++;
	}

	TupleDesc temp = new TupleDesc(typeAr, nameAr);
	return temp;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
        try {
	    if (((TupleDesc) o).numFields() == this.numFields()){
		for (int i = 0; i < this.numFields(); i++){
		    if (!(((TupleDesc) o).getFieldType(i)).equals(this.getFieldType(i))){
			return false;
		    }
		}
	    }
	    else {
		return false;
	    }
	}
	catch (Exception e){
	    if (o == null && this == null){
		return true;
	    }
	    else {
		return false;
	    }
	}
	return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
	Iterator items = this.iterator();
	String descriptor = "";
	while (items.hasNext()){
	    TDItem item = (TDItem) items.next();
	    descriptor += item.fieldType + "(" + item.fieldName + ")";
	}
	return descriptor;
    }
}
