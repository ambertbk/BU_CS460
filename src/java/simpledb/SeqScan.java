package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private int tableid;
    private String tableAlias;
    private DbFileIterator dbiterator;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        this.tid = tid;
        this.tableid = tableid;
        this.tableAlias = tableAlias;
        DbFile dbf = Database.getCatalog().getDatabaseFile(tableid);
        this.dbiterator = dbf.iterator(tid);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return null;
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias() {
        // some code goes here
        return this.tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
        this.tableid = tableid;
        this.tableAlias = tableAlias;
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        try {
            dbiterator.open();
        } catch (TransactionAbortedException e) {
            throw new TransactionAbortedException();
        }
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        TupleDesc to_change = Database.getCatalog().getDatabaseFile(tableid).getTupleDesc();
        int tuple_length = to_change.numFields();
        Type[] typeAr = new Type[tuple_length];
        String[] fieldAr = new String[tuple_length];
        for (int i=0; i<tuple_length; i++) {
            try {
                fieldAr[i] = tableAlias + "." + to_change.getFieldName(i);  //get the type name
                //then get the fieldType
                typeAr[i] = to_change.getFieldType(i);
            } catch (NoSuchElementException e) {
                fieldAr[i] = tableAlias + "." + "null";
                typeAr[i] = null;
            }
        }
        TupleDesc result = new TupleDesc(typeAr, fieldAr);
        return result;
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        try {
            if (dbiterator.hasNext()) {
                return true;
            } else {
                return false;
            }
        } catch (TransactionAbortedException e) {
            throw new TransactionAbortedException();
        } catch (DbException e) {
            throw new TransactionAbortedException();
        } catch (NoSuchElementException e) {
            throw new TransactionAbortedException();
        }
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        try {
            if (dbiterator.hasNext()) {
                Tuple result = dbiterator.next();
                return result;
            } else {
                throw new NoSuchElementException();
            }
        } catch (TransactionAbortedException e) {
            throw new NoSuchElementException();
        } catch (DbException e) {
            throw new NoSuchElementException();
        } catch (NoSuchElementException e) {
            throw new NoSuchElementException();
        }
    }

    public void close() {
        // some code goes here
        dbiterator.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        dbiterator.rewind();
    }
}