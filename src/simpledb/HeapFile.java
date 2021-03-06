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
    private TupleDesc td;

    private RandomAccessFile randomAccessFile;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.td = td;
        this.file = f;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return this.file.getAbsoluteFile().hashCode();
        // some code goes here

    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
        //throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int tableId = pid.getTableId();
        int pageNumber = pid.getPageNumber();
        //??????????????????????????????
        int offset = pageNumber* BufferPool.getPageSize();
        byte[] date = new byte[BufferPool.getPageSize()];
        try {
            randomAccessFile = new RandomAccessFile(this.file,"r");
            randomAccessFile.seek(offset);
            randomAccessFile.read(date,0,BufferPool.getPageSize());
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        try {
            randomAccessFile.close();
            return new HeapPage(new HeapPageId(tableId,pageNumber),date);
        } catch (IOException e) {
            throw new IllegalArgumentException();
        }
        // some code goes here;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        randomAccessFile = new RandomAccessFile(this.file,"rw");
        randomAccessFile.seek(page.getId().getPageNumber()*BufferPool.getPageSize());
        randomAccessFile.write(page.getPageData(),0,BufferPool.getPageSize());
        randomAccessFile.close();
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)this.file.length()/ BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        //???????????????
        ArrayList<Page> list = new ArrayList<>();
        for(int i =0 ;i<numPages();i++){
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), Permissions.READ_WRITE);
            if(page.getNumEmptySlots()>0){
                page.insertTuple(t);
                list.add(page);
                //Database.getBufferPool().releasePage(tid, page.getId());
                break;
            }
        }
        if(list.isEmpty()){
            //????????????,???????????????????????????
            byte[] emptyPageData = HeapPage.createEmptyPageData();
            HeapPage heapPage = new HeapPage(new HeapPageId(getId(),numPages()),emptyPageData);
            writePage(heapPage);
            HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, heapPage.getId(), Permissions.READ_WRITE);
            page.insertTuple(t);
            list.add(page);
            //Database.getBufferPool().releasePage(tid, page.getId());
        }
        return list;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        ArrayList<Page> list = new ArrayList<>();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid,t.getRecordId().getPageId(),Permissions.READ_WRITE);
        page.deleteTuple(t);
        list.add(page);
        //Database.getBufferPool().releasePage(tid,page.getId());
        return list;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileIterator() {
            private boolean isOpen=false;
            private Iterator<Tuple> tupleIterable;
            private HeapPage heapPage;
            private int pid = -1;
            private BufferPool bufferPool = Database.getBufferPool();
            private int currentPage = numPages();

            @Override
            public void open() throws DbException, TransactionAbortedException {
                pid = 0;
                tupleIterable = null;
            }
            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if(null!=tupleIterable&& tupleIterable.hasNext()){
                    return true;
                }else if(pid<0||pid>=currentPage){
                    return false;
                }else{
                    tupleIterable = ((HeapPage)bufferPool.getPage(tid, new HeapPageId(getId(),pid++), Permissions.READ_ONLY)).iterator();
                    return hasNext();
                }
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if(!hasNext()){
                    throw new DbException("no");
                }else{
                    return tupleIterable.next();
                }
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                pid=0;
                tupleIterable = null;
            }

            @Override
            public void close() {
                pid=-1;
                tupleIterable=null;
            }
        };
    }

}


