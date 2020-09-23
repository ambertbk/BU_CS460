package simpledb;

import com.sun.prism.impl.Disposer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    private TupleDesc tupleDesc;

    private Field[] fields;

    private RecordId recordId;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        tupleDesc = td;
        fields = new Field[td.numFields()];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        return recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        recordId = rid;
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
        if (!isValid(i)){
            throw new NoSuchElementException("invalid index");
        }
        fields[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        if (!isValid(i)){
            throw new IllegalArgumentException("invalid index");
        }
        return fields[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1 \t column2 \t column3 \t ... \t columnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
//        throw new UnsupportedOperationException("Implement this");
        StringBuffer rows = new StringBuffer();
        for (int i = 0; i < fields.length; i ++){
            if (i== fields.length - 1){
                rows.append(fields[i].toString() + "\n");
            } else {
                rows.append(fields[i].toString() + "\t");
            }
        }
        return rows.toString();
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        return new FieldIterator();
    }

    /**
     * reset the TupleDesc of the tuple
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        tupleDesc = td;
        fields = new Field[td.numFields()];

        for (int i = 0; i < td.numFields(); i ++){
            fields[i] = null;
        }

    }

    private boolean isValid(int index){
        if (fields != null){
            return (index >= 0 && index < fields.length);
        } else {
            return false;
        }
    }

    private class FieldIterator implements Iterator<Field>{

        private int pos = 0;

        @Override
        public boolean hasNext() {
            return fields.length > pos;
        }

        @Override
        public Field next() {
            if (!hasNext()){
                throw new NoSuchElementException("No fields!");
            }
            return fields[pos++];
        }
    }
}
