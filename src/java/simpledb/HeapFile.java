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

    private File file;
    private TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        file = f;
        tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
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
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int tableId = pid.getTableId();
        int pageNumber = pid.pageNumber();
        int size = Database.getBufferPool().getPageSize();

        byte[] data = HeapPage.createEmptyPageData();

        FileInputStream input;
        try {
            input = new FileInputStream(file);
            try{
                input.skip(pageNumber * size);
                input.read(data);
                return new HeapPage(new HeapPageId(tableId, pageNumber),data);
            }catch (IOException e){
                throw new IllegalArgumentException("");
            }
        } catch (IOException e){
            throw new IllegalArgumentException("HeapFile: ReadPage: file not found");
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        PageId pageId = page.getId();
//        int tableId = pageId.getTableId();
        int pNo = pageId.pageNumber();

        final int size = Database.getBufferPool().getPageSize();
        byte[] data = page.getPageData();

        RandomAccessFile dbfile = new RandomAccessFile(file,"rws");
        dbfile.skipBytes(pNo * size);
        dbfile.write(data);
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return ((int) file.length()) / Database.getBufferPool().getPageSize();
    }

    // see DbFile.java for javadocs
    /**
     * Inserts the specified tuple to the file on behalf of transaction.
     * This method will acquire a lock on the affected pages of the file, and
     * may block until the lock can be acquired.
     *
     * @param tid The transaction performing the update
     * @param t The tuple to add.  This tuple should be updated to reflect that
     *          it is now stored in this file.
     * @return An ArrayList contain the pages that were modified
     * @throws DbException if the tuple cannot be added
     * @throws IOException if the needed file can't be read/written
     */
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page> affected = new ArrayList<>(1);
        int numPages = numPages();

        for (int pageNo = 0; pageNo < numPages + 1; pageNo++){
            HeapPageId id = new HeapPageId(getId(), pageNo);
            HeapPage page;
            if (pageNo < numPages){
                page = (HeapPage) Database.getBufferPool().getPage(tid, id, Permissions.READ_WRITE);
            } else {
                page = new HeapPage(id, HeapPage.createEmptyPageData());
            }

            if (page.getNumEmptySlots() > 0){
                page.insertTuple(t);

                if(pageNo < numPages){
                    affected.add(page);
                } else {
                    writePage(page);
                }
                return affected;
            }
        }
        throw new DbException("HeapFile: InsertTuple: tuple cannot be added");
    }

    // see DbFile.java for javadocs
    /**
     * Removes the specified tuple from the file on behalf of the specified
     * transaction.
     * This method will acquire a lock on the affected pages of the file, and
     * may block until the lock can be acquired.
     *
     * @param tid The transaction performing the update
     * @param t The tuple to delete.  This tuple should be updated to reflect that
     *          it is no longer stored on any page.
     * @return An ArrayList contain the pages that were modified
     * @throws DbException if the tuple cannot be deleted or is not a member
     *   of the file
     */
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        ArrayList<Page> deleted = new ArrayList<>(1);
        RecordId rid = t.getRecordId();
        HeapPageId pid = (HeapPageId) rid.getPageId();
        if (pid.getTableId() == getId()){
//            int pageNo = pid.pageNumber();
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid,pid,Permissions.READ_WRITE);
            page.deleteTuple(t);
            deleted.add(page);
            return deleted;
        }
        throw new DbException("HeapFile: deleteTuple: the tuple cannot be deleted or is not a member of the file");
    }

    /**
     * Returns an iterator over all the tuples stored in this DbFile. The
     * iterator must use {@link BufferPool#getPage}, rather than
     * {@link #readPage} to iterate through the pages.
     *
     * @return an iterator over all the tuples stored in this DbFile.
     */
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid);
    }

    private class HeapFileIterator implements DbFileIterator{

        private Integer pgCursor;
        private Iterator<Tuple> tupleIterator;
        private final TransactionId transactionId;
        private final int tableId;
        private final int numPages;

        public HeapFileIterator(TransactionId tid){
            pgCursor = null;
            tupleIterator = null;
            transactionId = tid;
            tableId = getId();
            numPages = numPages();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            pgCursor = 0;
            tupleIterator = getTupleIterator(pgCursor);
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (pgCursor != null){
                while (pgCursor < numPages - 1){
                    if(tupleIterator.hasNext()){
                        return true;
                    } else {
                        pgCursor += 1;
                        tupleIterator = getTupleIterator(pgCursor);
                    }
                }
                return tupleIterator.hasNext();
            } else {
                return false;
            }
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (hasNext()) {
                return tupleIterator.next();
            }
            throw new NoSuchElementException("HeapFileIterator: no next tuple");
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            pgCursor = null;
            tupleIterator = null;
        }

        private Iterator<Tuple> getTupleIterator(int pageNo) throws TransactionAbortedException, DbException{
            PageId pageId = new HeapPageId(tableId, pageNo);
            return((HeapPage)Database.getBufferPool()
                                     .getPage(transactionId, pageId, Permissions.READ_ONLY)).iterator();
        }
    }
}

