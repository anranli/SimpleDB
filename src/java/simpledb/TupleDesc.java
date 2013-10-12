package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

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
			if (n == null) {
				n = "";
			} 
			this.fieldName = n;
			this.fieldType = t;
		}

		public String toString() {
			return fieldName + "(" + fieldType + ")";
		}
	}

	/**
	 * @return An iterator which iterates over all the field TDItems that are
	 *         included in this TupleDesc
	 * */
	public Iterator<TDItem> iterator() {
		return this.tupleArray.iterator();
	}

	private static final long serialVersionUID = 1L;

	ArrayList<TDItem> tupleArray = new ArrayList<TDItem>();

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
		for (int i = 0; i < typeAr.length; i++) {
			TDItem append = new TDItem(typeAr[i], fieldAr[i]);
			tupleArray.add(i, append);
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
		for (int i = 0; i < typeAr.length; i++) {
			TDItem append = new TDItem(typeAr[i], null);
			tupleArray.add(i, append);
		}
	}

	public TupleDesc() {
		this.tupleArray = new ArrayList<TDItem>();
	}

	/**
	 * @return the number of fields in this TupleDesc
	 */
	public int numFields() {
		return tupleArray.size();
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

		if (i > tupleArray.size()) {
			throw new NoSuchElementException("No such element in getFieldName.");
		}
		if (i < 0) {
			throw new NoSuchElementException("No such element in getFieldName.");
		}
		String arrayFieldName = tupleArray.get(i).fieldName;
		return arrayFieldName;
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
		if (i > tupleArray.size()) {
			throw new NoSuchElementException("No such element in getFieldType.");
		}
		if (i < 0) {
			throw new NoSuchElementException("No such element in getFieldType");
		}
		return tupleArray.get(i).fieldType;

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
		for (int i = 0; i < tupleArray.size(); i++) {
			if (tupleArray.get(i).fieldName != null
					&& tupleArray.get(i).fieldName.equals(name)) {
				return i;
			}
		}
		throw new NoSuchElementException("No such element in fieldNameToIndex.");
	}

	/**
	 * @return The size (in bytes) of tuples corresponding to this TupleDesc.
	 *         Note that tuples from a given TupleDesc are of a fixed size.
	 */
	public int getSize() {
		int size = 0;
		for (int i = 0; i < tupleArray.size(); i++) {
			if (tupleArray.get(i).fieldType.equals(Type.INT_TYPE)) {
				size += Type.INT_TYPE.getLen();
			} else if (tupleArray.get(i).fieldType.equals(Type.STRING_TYPE)) {
				size += Type.STRING_TYPE.getLen();
			}
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
		TupleDesc td3 = new TupleDesc();
		int totalLen = td1.tupleArray.size() + td2.tupleArray.size();
		td3.tupleArray = new ArrayList<TDItem>(totalLen);
		td3.tupleArray.addAll(0, td1.tupleArray);
		td3.tupleArray.addAll(td1.tupleArray.size(), td2.tupleArray);
		return td3;
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
		if (o == null || !(o instanceof TupleDesc)) {
			return false;
		}
		TupleDesc tupleObj = (TupleDesc) o;
		if (tupleObj.tupleArray.size() != tupleArray.size()) {
			return false;
		}
		for (int i = 0; i < tupleArray.size(); i++) {
			if (tupleObj.tupleArray.get(i).fieldType != tupleArray.get(i).fieldType) {
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
		String descriptor = "";
		for (int i = 0; i < tupleArray.size(); i++) {
			descriptor += tupleArray.get(i).fieldType + "("
					+ tupleArray.get(i).fieldName + "), ";
		}
		return descriptor;
	}
}
