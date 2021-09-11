package simpledb;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.sun.org.apache.bcel.internal.generic.LineNumberGen;
import com.sun.tools.internal.xjc.AbortException;

import simpledb.LockManager.WriteOrRead;

//import simpledb.LockManager.WriteOrRead;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    private int numPages;
    private Map<PageId, Page> pageMap;
    private LockManager lock;
    //实现lru
    LinkedList<PageId> linkByLru;

    public BufferPool(int numPages) {
        this.numPages = numPages;
        pageMap = new ConcurrentHashMap<>();
        linkByLru = new LinkedList<>();
        this.lock = new LockManager();
        // some code goes here
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // some code goes here
        try {
            Debug.log("{}获取{}的读锁111", tid, pid);
            if (perm.equals(Permissions.READ_ONLY)) {
                lock.acquireLock(tid, pid, WriteOrRead.READ);
            } else {
                lock.acquireLock(tid, pid, WriteOrRead.WRITE);
            }
            //System.out.println(tid+"获取的读锁222");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (!pageMap.containsKey(pid)) {
            DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
            if (pageMap.size() == numPages) {
                evictPage();
            }
            pageMap.put(pid, file.readPage(pid));
            //pageMap.get(pid).setBeforeImage();
            linkByLru.addFirst(pid);
        }
        linkByLru.remove(pid);
        linkByLru.addFirst(pid);
//        if (perm==Permissions.READ_WRITE)
//            pageMap.get(pid).markDirty(true, tid);
        //lock.releaseLock(tid,pid,WriteOrRead.READ);
        return pageMap.get(pid);
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lock.releaseLock(tid, pid, LockManager.WriteOrRead.READ);
        lock.releaseLock(tid, pid, LockManager.WriteOrRead.WRITE);

    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        transactionComplete(tid, true);

        // not necessary for lab1|lab2
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lock.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public synchronized void transactionComplete(TransactionId tid, boolean commit)
            throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        List<PageId> pageIds = lock.getpageId(tid);
        try {
            for (PageId pid : pageMap.keySet()) {
                if (pageMap.get(pid).isDirty() != null
                        && pageMap.get(pid).isDirty().equals(tid)) {
                    if (commit) {
                        pageMap.get(pid).setBeforeImage();
                        flushPage(pid);
                    } else {
                       // pageMap.put(pid, pageMap.get(pid).getBeforeImage());
                        discardPage(pid);
                    }
                }
            }
        }catch (NullPointerException e){
            e.printStackTrace();
            System.exit(0);
        }

        for (PageId pageId : pageIds) {
            releasePage(tid,pageId);
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        DbFile file =  Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> pages = file.insertTuple(tid, t);
        updateDirtyPage(pages, tid);
        // not necessary for lab1
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
                DbFile file =  Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
                ArrayList<Page> pages = file.deleteTuple(tid, t);
                updateDirtyPage(pages, tid);

    }

    public void updateDirtyPage(List<Page> pages, TransactionId tid) {
        for(Page page : pages) {
            page.markDirty(true, tid);
            PageId id = page.getId();
            if(!pageMap.containsKey(id)){
                if(pageMap.size()==numPages){
                    try {
                        evictPage();
                    } catch (DbException e) {
                        e.printStackTrace();
                    }
                }
                pageMap.put(id, page);
                linkByLru.addFirst(page.getId());
            }

        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        for (PageId pageId : this.pageMap.keySet()) {
            flushPage(pageId);
        }
        // not necessary for lab1

    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        if (pageMap.containsKey(pid)) {
            pageMap.remove(pid);
            linkByLru.remove(pid);
        }
        // not necessary for lab1
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        if(pageMap.containsKey(pid)){
            Page page = pageMap.get(pid);
            if(page.isDirty()!=null){
                Database.getLogFile().logWrite(page.isDirty(), page.getBeforeImage(), page);
                Database.getLogFile().force();

                DbFile databaseFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
                databaseFile.writePage(page);
                pageMap.remove(page.getId());
                linkByLru.remove(page.getId());
                page.markDirty(false, null);
                page.setBeforeImage();
            }
        }
        // not necessary for lab1
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (PageId pid : pageMap.keySet()) {
            Page p = pageMap.get(pid);
            if (p.isDirty() != null && p.isDirty().equals(tid)) {
                flushPage(pid);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */

    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1

                for (PageId pageId : linkByLru) {
                    Page   p   = pageMap.get(pageId);
                    if (p.isDirty() == null) {
                        // dont need to flushpage since all page evicted are not dirty
                        try {
                            flushPage(pageId);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        discardPage(pageId);
                        return;
                    }
                }
                throw new DbException("BufferPool: evictPage: all pages are marked as dirty");
            }
}

