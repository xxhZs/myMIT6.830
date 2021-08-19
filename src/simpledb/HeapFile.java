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
        //计算偏移量，偏移量是
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
        randomAccessFile.seek(numPages()*BufferPool.getPageSize());
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
        //查找空白页
        ArrayList<Page> list = new ArrayList<>();
        for(int i =0 ;i<numPages();i++){
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), Permissions.READ_ONLY);
            if(page.getNumEmptySlots()>0){
                page.insertTuple(t);
                list.add(page);
                break;
            }
        }
        if(list.isEmpty()){
            //没有空页,创建新页，然后插入
            byte[] emptyPageData = HeapPage.createEmptyPageData();
            HeapPage heapPage = new HeapPage(new HeapPageId(getId(),numPages()),emptyPageData);
            writePage(heapPage);
            HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, heapPage.getId(), Permissions.READ_ONLY);
            page.insertTuple(t);
            list.add(page);
        }
        return list;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        ArrayList<Page> list = new ArrayList<>();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid,t.getRecordId().getPageId(),Permissions.READ_ONLY);
        page.deleteTuple(t);
        list.add(page);
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
            private int pid = 0;
            private BufferPool bufferPool = Database.getBufferPool();
            private int currentPage = numPages();

            private boolean getNextTupleIterable(int pid)throws DbException {
                if(!isOpen){
                    throw new DbException("not open");
                }
                if(pid<0||pid>currentPage){
                    throw new DbException("not open");
                }else{
                    try {
                        heapPage = (HeapPage)bufferPool.getPage(tid, new HeapPageId(getId(), pid), Permissions.READ_ONLY);
                        if(heapPage==null){
                            return false;
                        }
                        tupleIterable = heapPage.iterator();
                    } catch (TransactionAbortedException e) {
                        e.printStackTrace();
                    } catch (DbException e) {
                        e.printStackTrace();
                    }
                }
                return true;
            }
            @Override
            public void open() throws DbException, TransactionAbortedException {
                isOpen = true;
                getNextTupleIterable(pid++);
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
//                if(tupleIterable==null){
//                    return false;
//                }else if(tupleIterable.hasNext()){
//                    return true;
//                }else{
//                    // get next iterator
//                    pid++;
//                    if(pid>=numPages()){
//                        return false;
//                    }else{
//                        getNextTupleIterable(pid);
//                        return hasNext();
//                    }
//                }
                return isOpen && pid < currentPage || (tupleIterable!=null&&pid == currentPage && tupleIterable.hasNext());
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if(!isOpen||tupleIterable==null){
                    throw new NoSuchElementException("expected exception");
                    //return null;
                }
                if(!tupleIterable.hasNext()){
                    //throw new NoSuchElementException("expected exception");
                    getNextTupleIterable(pid++);
                }
                return tupleIterable.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                close();
                open();
            }

            @Override
            public void close() {
                pid=0;
                heapPage=null;
                isOpen=false;
                tupleIterable=null;
            }
        };
    }

}

