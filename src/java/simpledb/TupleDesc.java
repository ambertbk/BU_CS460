package simpledb;

import java.io.Serializable;
import java.util.*;
import java.util.Iterator;
import java.util.ArrayList;

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
        public final Type fieldType;

        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    private ArrayList<TDItem> item_array;  //create an arraybag of TDItem

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator(TDItem items) {
        // some code goes here
        //return an iterator item of array, used javs's iterator class
        return item_array.iterator();
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
        // some code goes here
        //construct the item_array according to the description given
        int arrlength = typeAr.length;
        item_array = new ArrayList<TDItem>();  //create an array of tditems with fixed size
        for (int i=0; i<arrlength; i++) {
            TDItem x = new TDItem(typeAr[i], fieldAr[i]);
            item_array.add(x);  //construct the array according to description
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
        // some code goes here
        this(typeAr, new String[typeAr.length]);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        //the length of the item_array, the tuples, is equal to the fields inside the array,
        //use size() to get length
        //System.out.println(item_array + " at numFields");
        if (item_array == null) {
            return 0;
        }
        return (item_array.size());
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
        try {
            String result = item_array.get(i).fieldName;  //return the fieldname of ith object
            return result;
        } catch (NullPointerException e) {
            throw new NoSuchElementException();
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
        try {
            Type result = item_array.get(i).fieldType;
            return result;
        } catch (NullPointerException e) {
            throw new NoSuchElementException();
        }
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
        //System.out.println("name = " + name);
        //System.out.println("items = " + item_array);
        //System.out.println("null.equals(test) " + "test".equals(null));

        //go through the entire list using iterator, return index if name found
        if (item_array == null) {throw new NoSuchElementException();}
        if (name == null) {throw new NoSuchElementException();}
        for (int i=0; i<item_array.size();i++) {
            String name_check = item_array.get(i).fieldName;
            //System.out.println("name_check = " + name_check);
            //System.out.println("check: " + (name.equals(name_check)));
            if (name.equals(name_check)) {
                //System.out.println("name_check = " + name_check);
                return i;
            }
            //System.out.println(item_array + " size = "+ item_array.size() + " name = " + name + " name_check = " + name_check);
        }
        //cannot find the element
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        // add all element: length = byte_size *  length
        //System.out.println(item_array + "at getSize");
        int length = 0;
        if (item_array == null) {
            return 0;
        }
        for (int i=0; i<item_array.size(); i++) {
            length = length + getFieldType(i).getLen();
        }
        return length;
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
        //System.out.println("td1 = " + td1.item_array);
        //System.out.println("td2 = " + td2.item_array);

        // we append each type and each file to seperate array and construct a new array
        int length = td1.item_array.size() + td2.item_array.size();
        int length1 = td1.item_array.size();
        int length2 = td2.item_array.size();
        Type[] typearray = new Type[length];
        String[] namearray = new String[length];

        for (int i=0; i<length1; i++) {
            String x = td1.item_array.get(i).fieldName;
            Type y = td1.item_array.get(i).fieldType;
            typearray[i] = y;
            namearray[i] = x;
        }

        for (int i=0; i<length2; i++) {
            String x = td2.item_array.get(i).fieldName;
            Type y = td2.item_array.get(i).fieldType;
            typearray[i + length1] = y;
            namearray[i + length1] = x;
        }

        //combine all these useing constructor
        //TupleDesc(typearray,namearray);
        TupleDesc result = new TupleDesc(typearray,namearray);
        //System.out.println("result = " + result.item_array);
        return (result);
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
        //test of o is a tupleDesc
        //System.out.println("item_array: " + item_array);
        if (o == null && item_array == null) {return true;}
        if (!(o instanceof TupleDesc)) { return false; }  // if o is not a ArrayList<TDItem> object
        if (!(((TupleDesc) o).item_array.size() == item_array.size())) { return false; }

        for (int i=0; i<item_array.size();i++) {
            if (!(item_array.get(i).fieldType.equals(((TupleDesc) o).item_array.get(i).fieldType))) {
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
        String result = "";
        //System.out.println("item_array = " + item_array);
        for (int i=0;i<item_array.size();i++) {
            String s = Integer.toString(i);
            //System.out.println("s = " + s);
            //String q = Type.toString(item_array.get(i).fieldType);
            StringBuilder y = new StringBuilder();
            y.append((item_array.get(i).fieldType).toString());
            String q = y.toString();

            //System.out.println("q = " + q);

            result = result + q + "[" + s + "]";
            result = result + (item_array.get(i).fieldName) + "[" + s + "]";
            //System.out.println("result22 = " + result);
        }
        //System.out.println("result22 = " + result);
        return result;
    }
}
