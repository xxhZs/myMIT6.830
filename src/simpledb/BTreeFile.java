package simpledb;

import java.io.*;
import java.util.*;

import simpledb.Predicate.Op;

/**
 * BTreeFile is an implementation of a DbFile that stores a B+ tree.
 * Specifically, it stores a pointer to a root page,
 * a set of internal pages, and a set of leaf pages, which contain a collection of tuples
 * in sorted order. BTreeFile works closely with BTreeLeafPage, BTreeInternalPage,
 * and BTreeRootPtrPage. The format of these pages is described in their constructors.
 *
 * @author Becca Taft
 * @see simpledb.BTreeLeafPage#BTreeLeafPage
 * @see simpledb.BTreeInternalPage#BTreeInternalPage
 * @see simpledb.BTreeHeaderPage#BTreeHeaderPage
 * @see simpledb.BTreeRootPtrPage#BTreeRootPtrPage
 */
public class BTreeFile implements DbFile {

    private final File f;
    private final TupleDesc td;
    private final int tableid;
    private int keyField;

    /**
     * Constructs a B+ tree file backed by the specified file.
     *
     * @param f - the file that stores the on-disk backing store for this B+ tree
     * file.
     * @param key - the field which index is keyed on
     * @param td - the tuple descriptor of tuples in the file
     */
    public BTreeFile(File f, int key, TupleDesc td) {
        this.f = f;
        this.tableid = f.getAbsoluteFile().hashCode();
        this.keyField = key;
        this.td = td;
    }

    /**
     * Returns the File backing this BTreeFile on disk.
     */
    public File getFile() {
        return f;
    }

    /**
     * Returns an ID uniquely identifying this BTreeFile. Implementation note:
     * you will need to generate this tableid somewhere and ensure that each
     * BTreeFile has a "unique id," and that you always return the same value for
     * a particular BTreeFile. We suggest hashing the absolute file name of the
     * file underlying the BTreeFile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this BTreeFile.
     */
    public int getId() {
        return tableid;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    /**
     * Read a page from the file on disk. This should not be called directly
     * but should be called from the BufferPool via getPage()
     *
     * @param pid - the id of the page to read from disk
     * @return the page constructed from the contents on disk
     */
    public Page readPage(PageId pid) {
        BTreePageId id = (BTreePageId) pid;
        BufferedInputStream bis = null;

        try {
            bis = new BufferedInputStream(new FileInputStream(f));
            if (id.pgcateg() == BTreePageId.ROOT_PTR) {
                byte pageBuf[] = new byte[BTreeRootPtrPage.getPageSize()];
                int retval = bis.read(pageBuf, 0, BTreeRootPtrPage.getPageSize());
                if (retval == -1) {
                    throw new IllegalArgumentException("Read past end of table");
                }
                if (retval < BTreeRootPtrPage.getPageSize()) {
                    throw new IllegalArgumentException("Unable to read "
                            + BTreeRootPtrPage.getPageSize() + " bytes from BTreeFile");
                }
                Debug.log(1, "BTreeFile.readPage: read page %d", id.getPageNumber());
                BTreeRootPtrPage p = new BTreeRootPtrPage(id, pageBuf);
                return p;
            } else {
                byte pageBuf[] = new byte[BufferPool.getPageSize()];
                if (bis.skip(BTreeRootPtrPage.getPageSize() + (id.getPageNumber() - 1) * BufferPool.getPageSize()) !=
                        BTreeRootPtrPage.getPageSize() + (id.getPageNumber() - 1) * BufferPool.getPageSize()) {
                    throw new IllegalArgumentException(
                            "Unable to seek to correct place in BTreeFile");
                }
                int retval = bis.read(pageBuf, 0, BufferPool.getPageSize());
                if (retval == -1) {
                    throw new IllegalArgumentException("Read past end of table");
                }
                if (retval < BufferPool.getPageSize()) {
                    throw new IllegalArgumentException("Unable to read "
                            + BufferPool.getPageSize() + " bytes from BTreeFile");
                }
                Debug.log(1, "BTreeFile.readPage: read page %d", id.getPageNumber());
                if (id.pgcateg() == BTreePageId.INTERNAL) {
                    BTreeInternalPage p = new BTreeInternalPage(id, pageBuf, keyField);
                    return p;
                } else if (id.pgcateg() == BTreePageId.LEAF) {
                    BTreeLeafPage p = new BTreeLeafPage(id, pageBuf, keyField);
                    return p;
                } else { // id.pgcateg() == BTreePageId.HEADER
                    BTreeHeaderPage p = new BTreeHeaderPage(id, pageBuf);
                    return p;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            // Close the file on success or error
            try {
                if (bis != null) {
                    bis.close();
                }
            } catch (IOException ioe) {
                // Ignore failures closing the file
            }
        }
    }

    /**
     * Write a page to disk.  This should not be called directly but should
     * be called from the BufferPool when pages are flushed to disk
     *
     * @param page - the page to write to disk
     */
    public void writePage(Page page) throws IOException {
        BTreePageId id = (BTreePageId) page.getId();

        byte[] data = page.getPageData();
        RandomAccessFile rf = new RandomAccessFile(f, "rw");
        if (id.pgcateg() == BTreePageId.ROOT_PTR) {
            rf.write(data);
            rf.close();
        } else {
            rf.seek(BTreeRootPtrPage.getPageSize() + (page.getId().getPageNumber() - 1) * BufferPool.getPageSize());
            rf.write(data);
            rf.close();
        }
    }

    /**
     * Returns the number of pages in this BTreeFile.
     */
    public int numPages() {
        // we only ever write full pages
        return (int) ((f.length() - BTreeRootPtrPage.getPageSize()) / BufferPool.getPageSize());
    }

    /**
     * Returns the index of the field that this B+ tree is keyed on
     */
    public int keyField() {
        return keyField;
    }

    /**
     * Recursive function which finds and locks the leaf page in the B+ tree corresponding to
     * the left-most page possibly containing the key field f. It locks all internal
     * nodes along the path to the leaf node with READ_ONLY permission, and locks the
     * leaf node with permission perm.
     * <p>
     * If f is null, it finds the left-most leaf page -- used for the iterator
     *
     * @param tid - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param pid - the current page being searched
     * @param perm - the permissions with which to lock the leaf page
     * @param f - the field to search for
     * @return the left-most leaf page possibly containing the key field f
     */
    private BTreeLeafPage findLeafPage(TransactionId tid, HashMap<PageId, Page> dirtypages, BTreePageId pid,
            Permissions perm,
            Field f)
            throws DbException, TransactionAbortedException {
        // some code goes here
        if (pid.pgcateg() == BTreePageId.LEAF) {
            //是叶子节点直接返回
            return (BTreeLeafPage) this.getPage(tid, dirtypages, pid, perm);
        } else {
            //不是叶子节点,查找比他大的，然后向下找
            BTreePageId nextPageId;
            BTreeInternalPage findPage = (BTreeInternalPage) this.getPage(tid, dirtypages, pid, Permissions.READ_ONLY);

            BTreeEntry entry;
            Iterator<BTreeEntry> iterator = findPage.iterator();
            if (iterator.hasNext()) {
                entry = iterator.next();
            } else {
                throw new DbException("没有子节点了");
            }
            if (f == null) {
                nextPageId = entry.getLeftChild();
            } else {
                while (f.compare(Op.GREATER_THAN, entry.getKey()) && iterator.hasNext()) {
                    entry = iterator.next();
                }
                if (f.compare(Op.LESS_THAN_OR_EQ, entry.getKey())) {
                    nextPageId = entry.getLeftChild();

                } else {
                    //					nextPageId = entry.getLeftChild();
                    nextPageId = entry.getRightChild();
                }
            }
            return findLeafPage(tid, dirtypages, nextPageId, perm, f);
        }
    }

    /**
     * Convenience method to find a leaf page when there is no dirtypages HashMap.
     * Used by the BTreeFile iterator.
     *
     * @param tid - the transaction id
     * @param pid - the current page being searched
     * @param perm - the permissions with which to lock the leaf page
     * @param f - the field to search for
     * @return the left-most leaf page possibly containing the key field f
     * @see #findLeafPage(TransactionId, HashMap, BTreePageId, Permissions, Field)
     */
    BTreeLeafPage findLeafPage(TransactionId tid, BTreePageId pid, Permissions perm,
            Field f)
            throws DbException, TransactionAbortedException {
        return findLeafPage(tid, new HashMap<PageId, Page>(), pid, perm, f);
    }

    /**
     * Split a leaf page to make room for new tuples and recursively split the parent node
     * as needed to accommodate a new entry. The new entry should have a key matching the key field
     * of the first tuple in the right-hand page (the key is "copied up"), and child pointers
     * pointing to the two leaf pages resulting from the split.  Update sibling pointers and parent
     * pointers as needed.
     * <p>
     * Return the leaf page into which a new tuple with key field "field" should be inserted.
     *
     * @param tid - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param page - the leaf page to split
     * @param field - the key field of the tuple to be inserted after the split is complete. Necessary to know
     * which of the two pages to return.
     * @return the leaf page into which the new tuple should be inserted
     * @see #getParentWithEmptySlots(TransactionId, HashMap, BTreePageId, Field)
     */
    protected BTreeLeafPage splitLeafPage(TransactionId tid, HashMap<PageId, Page> dirtypages, BTreeLeafPage page,
            Field field)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        //
        // Split the leaf page by adding a new page on the right of the existing
        // page and moving half of the tuples to the new page.  Copy the middle key up
        // into the parent page, and recursively split the parent as needed to accommodate
        // the new entry.  getParentWithEmtpySlots() will be useful here.  Don't forget to update
        // the sibling pointers of all the affected leaf pages.  Return the page into which a
        // tuple with the given key field should be inserted.

        //分裂叶子节点的方法
        //1.添加新页到当前页面到当前页面到右边
        //2 复制一半到页面到旁边，并且上升到上级页面
        //3 分裂上级页面
        //3 1 假如上级页面也需要分裂，使用getparent方法做上级分裂到准备
        //32 进行分裂上级


        //1 左右节点到双指针
        BTreeLeafPage newLeafPage = (BTreeLeafPage) getEmptyPage(tid, dirtypages, BTreePageId.LEAF);


        //2. 移动节点一半到数据到新节点
        Iterator<Tuple> tupleIterator = page.reverseIterator();
        int n = page.getNumTuples() / 2;
        int i = 0;
        Tuple lastTuple = null;
       // Stack<Tuple> stack = new Stack<>();
        while(tupleIterator.hasNext()){
            if(i < n){
                Tuple tuple = tupleIterator.next();
                page.deleteTuple(tuple);
                newLeafPage.insertTuple(tuple);
                i++;
            }else{
                lastTuple = tupleIterator.next();
                break;
            }
        }
        if(page.getRightSiblingId()!=null){
            BTreeLeafPage oledRightPage =
                    (BTreeLeafPage) getPage(tid, dirtypages, page.getRightSiblingId(), Permissions.READ_WRITE);
            newLeafPage.setRightSiblingId(oledRightPage.getId());
            oledRightPage.setLeftSiblingId(newLeafPage.getId());
            dirtypages.put(oledRightPage.getId(),oledRightPage);
        }
        page.setRightSiblingId(newLeafPage.getId());
        newLeafPage.setLeftSiblingId(page.getId());
//        while(!stack.isEmpty()){
//            lastTuple  = stack.pop();
//            newLeafPage.insertTuple(lastTuple);
//        }
        //3 然后上升到父节点，在父节点里会进行树的父节点分裂，
        Field insertField = lastTuple.getField(keyField);
        BTreeInternalPage parentWithEmptySlots =
                getParentWithEmptySlots(tid, dirtypages, page.getParentId(), insertField);
        parentWithEmptySlots = (BTreeInternalPage) getPage(tid, dirtypages, parentWithEmptySlots.getId(), Permissions.READ_WRITE);
        newLeafPage.setParentId(parentWithEmptySlots.getId());
        page.setParentId(parentWithEmptySlots.getId());

        //3 1 将记录更新到父节点
        BTreeEntry entry = new BTreeEntry(insertField,page.getId(),newLeafPage.getId());
        parentWithEmptySlots.insertEntry(entry);
        updateParentPointers(tid,dirtypages,parentWithEmptySlots);

        //4 更新脏页
        dirtypages.put(parentWithEmptySlots.getId(),parentWithEmptySlots);
        dirtypages.put(page.getId(),page);
        dirtypages.put(newLeafPage.getId(),newLeafPage);

        // 5
        if(field.compare(Op.GREATER_THAN , insertField)){
            return newLeafPage;
        }else{
            return page;
        }

    }

    /**
     * Split an internal page to make room for new entries and recursively split its parent page
     * as needed to accommodate a new entry. The new entry for the parent should have a key matching
     * the middle key in the original internal page being split (this key is "pushed up" to the parent).
     * The child pointers of the new parent entry should point to the two internal pages resulting
     * from the split. Update parent pointers as needed.
     * <p>
     * Return the internal page into which an entry with key field "field" should be inserted
     *
     * @param tid - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param page - the internal page to split
     * @param field - the key field of the entry to be inserted after the split is complete. Necessary to know
     * which of the two pages to return.
     * @return the internal page into which the new entry should be inserted
     * @see #getParentWithEmptySlots(TransactionId, HashMap, BTreePageId, Field)
     * @see #updateParentPointers(TransactionId, HashMap, BTreeInternalPage)
     */
    protected BTreeInternalPage splitInternalPage(TransactionId tid, HashMap<PageId, Page> dirtypages,
            BTreeInternalPage page, Field field)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        //
        // Split the internal page by adding a new page on the right of the existing
        // page and moving half of the entries to the new page.  Push the middle key up
        // into the parent page, and recursively split the parent as needed to accommodate
        // the new entry.  getParentWithEmtpySlots() will be useful here.  Don't forget to update
        // the parent pointers of all the children moving to the new page.  updateParentPointers()
        // will be useful here.  Return the page into which an entry with the given key field
        // should be inserted.

        //1.先进行新建与数据复制
        Iterator<BTreeEntry> bTreeEntryIterator = page.reverseIterator();
        BTreeInternalPage newPage = (BTreeInternalPage)getEmptyPage(tid, dirtypages, BTreePageId.INTERNAL);
        int n = page.getNumEntries() / 2;
        int i = 0;
        BTreeEntry bTreeEntry = null ;
        while(bTreeEntryIterator.hasNext()){
            if(i < n){
                BTreeEntry next = bTreeEntryIterator.next();
                page.deleteKeyAndRightChild(next);
                newPage.insertEntry(next);
                i++;
                updateParentPointer(tid,dirtypages, newPage.getId(), next.getRightChild());
            }else{
                bTreeEntry = bTreeEntryIterator.next();
                System.out.println(bTreeEntry);
                page.deleteKeyAndRightChild(bTreeEntry);
                break;
            }
        }
        //2 向父节点递归
        Field key = bTreeEntry.getKey();
        BTreeEntry bTreeEntry1 = new BTreeEntry(key, page.getId(), newPage.getId());
        BTreeInternalPage parentWithEmptySlots = getParentWithEmptySlots(tid, dirtypages,
                page.getParentId(), key);

        updateParentPointers(tid,dirtypages,page);
        updateParentPointers(tid,dirtypages,newPage);

        parentWithEmptySlots = (BTreeInternalPage) getPage(tid, dirtypages, parentWithEmptySlots.getId(), Permissions.READ_WRITE);
        parentWithEmptySlots.insertEntry(bTreeEntry1);
        page.setParentId(parentWithEmptySlots.getId());
        newPage.setParentId(parentWithEmptySlots.getId());

        updateParentPointers(tid,dirtypages,parentWithEmptySlots);
        //3 写脏页
        dirtypages.put(parentWithEmptySlots.getId(),parentWithEmptySlots);
        dirtypages.put(page.getId(),page);
        dirtypages.put(newPage.getId(),newPage);

        if(field.compare(Op.GREATER_THAN , key)){
            return newPage;
        }else{
            return page;
        }
    }

    /**
     * Method to encapsulate the process of getting a parent page ready to accept new entries.
     * This may mean creating a page to become the new root of the tree, splitting the existing
     * parent page if there are no empty slots, or simply locking and returning the existing parent page.
     *
     * @param tid - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param parentId - the id of the parent. May be an internal page or the RootPtr page
     * @param field - the key of the entry which will be inserted. Needed in case the parent must be split
     * to accommodate the new entry
     * @return the parent page, guaranteed to have at least one empty slot
     * @see #splitInternalPage(TransactionId, HashMap, BTreeInternalPage, Field)
     */
    private BTreeInternalPage getParentWithEmptySlots(TransactionId tid, HashMap<PageId, Page> dirtypages,
            BTreePageId parentId, Field field) throws DbException, IOException, TransactionAbortedException {

        BTreeInternalPage parent = null;

        // create a parent node if necessary
        // this will be the new root of the tree
        if (parentId.pgcateg() == BTreePageId.ROOT_PTR) {
            parent = (BTreeInternalPage) getEmptyPage(tid, dirtypages, BTreePageId.INTERNAL);

            // update the root pointer
            BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) getPage(tid, dirtypages,
                    BTreeRootPtrPage.getId(tableid), Permissions.READ_WRITE);
            BTreePageId prevRootId = rootPtr.getRootId(); //save prev id before overwriting.
            rootPtr.setRootId(parent.getId());

            // update the previous root to now point to this new root.
            BTreePage prevRootPage = (BTreePage) getPage(tid, dirtypages, prevRootId, Permissions.READ_WRITE);
            prevRootPage.setParentId(parent.getId());
        } else {
            // lock the parent page
            parent = (BTreeInternalPage) getPage(tid, dirtypages, parentId,
                    Permissions.READ_WRITE);
        }

        // split the parent if needed
        if (parent.getNumEmptySlots() == 0) {
            parent = splitInternalPage(tid, dirtypages, parent, field);
        }

        return parent;

    }

    /**
     * Helper function to update the parent pointer of a node.
     *
     * @param tid - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param pid - id of the parent node
     * @param child - id of the child node to be updated with the parent pointer
     */
    private void updateParentPointer(TransactionId tid, HashMap<PageId, Page> dirtypages, BTreePageId pid,
            BTreePageId child)
            throws DbException, IOException, TransactionAbortedException {

        BTreePage p = (BTreePage) getPage(tid, dirtypages, child, Permissions.READ_ONLY);

        if (!p.getParentId().equals(pid)) {
            p = (BTreePage) getPage(tid, dirtypages, child, Permissions.READ_WRITE);
            p.setParentId(pid);
        }

    }

    /**
     * Update the parent pointer of every child of the given page so that it correctly points to
     * the parent
     *
     * @param tid - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param page - the parent page
     * @see #updateParentPointer(TransactionId, HashMap, BTreePageId, BTreePageId)
     */
    private void updateParentPointers(TransactionId tid, HashMap<PageId, Page> dirtypages, BTreeInternalPage page)
            throws DbException, IOException, TransactionAbortedException {
        Iterator<BTreeEntry> it = page.iterator();
        BTreePageId pid = page.getId();
        BTreeEntry e = null;
        while (it.hasNext()) {
            e = it.next();
            updateParentPointer(tid, dirtypages, pid, e.getLeftChild());
        }
        if (e != null) {
            updateParentPointer(tid, dirtypages, pid, e.getRightChild());
        }
    }

    /**
     * Method to encapsulate the process of locking/fetching a page.  First the method checks the local
     * cache ("dirtypages"), and if it can't find the requested page there, it fetches it from the buffer pool.
     * It also adds pages to the dirtypages cache if they are fetched with read-write permission, since
     * presumably they will soon be dirtied by this transaction.
     * <p>
     * This method is needed to ensure that page updates are not lost if the same pages are
     * accessed multiple times.
     *
     * @param tid - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param pid - the id of the requested page
     * @param perm - the requested permissions on the page
     * @return the requested page
     */
    Page getPage(TransactionId tid, HashMap<PageId, Page> dirtypages, BTreePageId pid, Permissions perm)
            throws DbException, TransactionAbortedException {
        if (dirtypages.containsKey(pid)) {
            return dirtypages.get(pid);
        } else {
            Page p = Database.getBufferPool().getPage(tid, pid, perm);
            if (perm == Permissions.READ_WRITE) {
                dirtypages.put(pid, p);
            }

            return p;
        }
    }

    /**
     * Insert a tuple into this BTreeFile, keeping the tuples in sorted order.
     * May cause pages to split if the page where tuple t belongs is full.
     *
     * @param tid - the transaction id
     * @param t - the tuple to insert
     * @return a list of all pages that were dirtied by this operation. Could include
     * many pages since parent pointers will need to be updated when an internal node splits.
     * @see #splitLeafPage(TransactionId, HashMap, BTreeLeafPage, Field)
     */
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        HashMap<PageId, Page> dirtypages = new HashMap<PageId, Page>();

        // get a read lock on the root pointer page and use it to locate the root page
        BTreeRootPtrPage rootPtr = getRootPtrPage(tid, dirtypages);
        BTreePageId rootId = rootPtr.getRootId();

        if (rootId == null) { // the root has just been created, so set the root pointer to point to it
            rootId = new BTreePageId(tableid, numPages(), BTreePageId.LEAF);
            rootPtr = (BTreeRootPtrPage) getPage(tid, dirtypages, BTreeRootPtrPage.getId(tableid),
                    Permissions.READ_WRITE);
            rootPtr.setRootId(rootId);
        }

        // find and lock the left-most leaf page corresponding to the key field,
        // and split the leaf page if there are no more slots available
        BTreeLeafPage leafPage = findLeafPage(tid, dirtypages, rootId, Permissions.READ_WRITE, t.getField(keyField));
        if (leafPage.getNumEmptySlots() == 0) {
            leafPage = splitLeafPage(tid, dirtypages, leafPage, t.getField(keyField));
        }

        // insert the tuple into the leaf page
        leafPage.insertTuple(t);

        ArrayList<Page> dirtyPagesArr = new ArrayList<Page>();
        dirtyPagesArr.addAll(dirtypages.values());
        return dirtyPagesArr;
    }

    /**
     * Handle the case when a B+ tree page becomes less than half full due to deletions.
     * If one of its siblings has extra tuples/entries, redistribute those tuples/entries.
     * Otherwise merge with one of the siblings. Update pointers as needed.
     *
     * @param tid - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param page - the page which is less than half full
     * @see #handleMinOccupancyLeafPage(TransactionId, HashMap, BTreeLeafPage, BTreeInternalPage, BTreeEntry, BTreeEntry)
     * @see #handleMinOccupancyInternalPage(TransactionId, HashMap, BTreeInternalPage, BTreeInternalPage, BTreeEntry, BTreeEntry)
     */
    private void handleMinOccupancyPage(TransactionId tid, HashMap<PageId, Page> dirtypages, BTreePage page)
            throws DbException, IOException, TransactionAbortedException {
        BTreePageId parentId = page.getParentId();
        BTreeEntry leftEntry = null;
        BTreeEntry rightEntry = null;
        BTreeInternalPage parent = null;

        // find the left and right siblings through the parent so we make sure they have
        // the same parent as the page. Find the entries in the parent corresponding to
        // the page and siblings
        if (parentId.pgcateg() != BTreePageId.ROOT_PTR) {
            //父节点不是根节点
            parent = (BTreeInternalPage) getPage(tid, dirtypages, parentId, Permissions.READ_WRITE);
            Iterator<BTreeEntry> ite = parent.iterator();
            while (ite.hasNext()) {
                BTreeEntry e = ite.next();
                if (e.getLeftChild().equals(page.getId())) {
                    rightEntry = e;
                    break;
                } else if (e.getRightChild().equals(page.getId())) {
                    leftEntry = e;
                }
            }
        }

        if (page.getId().pgcateg() == BTreePageId.LEAF) {
            handleMinOccupancyLeafPage(tid, dirtypages, (BTreeLeafPage) page, parent, leftEntry, rightEntry);
        } else { // BTreePageId.INTERNAL
            handleMinOccupancyInternalPage(tid, dirtypages, (BTreeInternalPage) page, parent, leftEntry, rightEntry);
        }
    }

    /**
     * Handle the case when a leaf page becomes less than half full due to deletions.
     * If one of its siblings has extra tuples, redistribute those tuples.
     * Otherwise merge with one of the siblings. Update pointers as needed.
     *
     * @param tid - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param page - the leaf page which is less than half full
     * @param parent - the parent of the leaf page
     * @param leftEntry - the entry in the parent pointing to the given page and its left-sibling
     * @param rightEntry - the entry in the parent pointing to the given page and its right-sibling
     * @see #mergeLeafPages(TransactionId, HashMap, BTreeLeafPage, BTreeLeafPage, BTreeInternalPage, BTreeEntry)
     * @see #stealFromLeafPage(BTreeLeafPage, BTreeLeafPage, BTreeInternalPage, BTreeEntry, boolean)
     */
    private void handleMinOccupancyLeafPage(TransactionId tid, HashMap<PageId, Page> dirtypages, BTreeLeafPage page,
            BTreeInternalPage parent, BTreeEntry leftEntry, BTreeEntry rightEntry)
            throws DbException, IOException, TransactionAbortedException {
        BTreePageId leftSiblingId = null;
        BTreePageId rightSiblingId = null;
        if (leftEntry != null) {
            leftSiblingId = leftEntry.getLeftChild();
        }
        if (rightEntry != null) {
            rightSiblingId = rightEntry.getRightChild();
        }

        int maxEmptySlots = page.getMaxTuples() - page.getMaxTuples() / 2; // ceiling
        if (leftSiblingId != null) {
            BTreeLeafPage leftSibling = (BTreeLeafPage) getPage(tid, dirtypages, leftSiblingId, Permissions.READ_WRITE);
            // if the left sibling is at minimum occupancy, merge with it. Otherwise
            // steal some tuples from it
            if (leftSibling.getNumEmptySlots() >= maxEmptySlots) {
                mergeLeafPages(tid, dirtypages, leftSibling, page, parent, leftEntry);
            } else {
                stealFromLeafPage(page, leftSibling, parent, leftEntry, false);
            }
        } else if (rightSiblingId != null) {
            BTreeLeafPage rightSibling =
                    (BTreeLeafPage) getPage(tid, dirtypages, rightSiblingId, Permissions.READ_WRITE);
            // if the right sibling is at minimum occupancy, merge with it. Otherwise
            // steal some tuples from it
            if (rightSibling.getNumEmptySlots() >= maxEmptySlots) {
                mergeLeafPages(tid, dirtypages, page, rightSibling, parent, rightEntry);
            } else {
                stealFromLeafPage(page, rightSibling, parent, rightEntry, true);
            }
        }
    }

    /**
     * Steal tuples from a sibling and copy them to the given page so that both pages are at least
     * half full.  Update the parent's entry so that the key matches the key field of the first
     * tuple in the right-hand page.
     *
     * @param page - the leaf page which is less than half full
     * @param sibling - the sibling which has tuples to spare
     * @param parent - the parent of the two leaf pages
     * @param entry - the entry in the parent pointing to the two leaf pages
     * @param isRightSibling - whether the sibling is a right-sibling
     */
    protected void stealFromLeafPage(BTreeLeafPage page, BTreeLeafPage sibling,
            BTreeInternalPage parent, BTreeEntry entry, boolean isRightSibling) throws DbException {
        // some code goes here
        //
        // Move some of the tuples from the sibling to the page so
        // that the tuples are evenly distributed. Be sure to update
        // the corresponding parent entry.

        //从兄弟复制元素　
        BTreeLeafPage ths ;
        int numTupleToMove = (sibling.getNumTuples() - page.getNumTuples()) / 2;
        Tuple[] tp = new Tuple[numTupleToMove];
        int cnt = tp.length - 1;
        Iterator<Tuple> it;
        //左右的区别是给的值不一样
        if(isRightSibling){
            ths = sibling;
            it = sibling.iterator();//1 2 3
        }else{
            ths = page;
            it = sibling.reverseIterator();//3 2 1
        }
        while(cnt >=0 && it.hasNext()){
            tp[cnt--] = it.next();
        }
        for(Tuple t : tp){
            sibling.deleteTuple(t);
            page.insertTuple(t);
        }
        //更新父节点内的值
        if(ths.getNumTuples() > 0){
            entry.setKey(ths.iterator().next().getField(keyField));
            parent.updateEntry(entry);
        }

    }

    /**
     * Handle the case when an internal page becomes less than half full due to deletions.
     * If one of its siblings has extra entries, redistribute those entries.
     * Otherwise merge with one of the siblings. Update pointers as needed.
     *
     * @param tid - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param page - the internal page which is less than half full
     * @param parent - the parent of the internal page
     * @param leftEntry - the entry in the parent pointing to the given page and its left-sibling
     * @param rightEntry - the entry in the parent pointing to the given page and its right-sibling
     * @see #mergeInternalPages(TransactionId, HashMap, BTreeInternalPage, BTreeInternalPage, BTreeInternalPage, BTreeEntry)
     * @see #stealFromLeftInternalPage(TransactionId, HashMap, BTreeInternalPage, BTreeInternalPage, BTreeInternalPage, BTreeEntry)
     * @see #stealFromRightInternalPage(TransactionId, HashMap, BTreeInternalPage, BTreeInternalPage, BTreeInternalPage, BTreeEntry)
     */
    private void handleMinOccupancyInternalPage(TransactionId tid, HashMap<PageId, Page> dirtypages,
            BTreeInternalPage page, BTreeInternalPage parent, BTreeEntry leftEntry, BTreeEntry rightEntry)
            throws DbException, IOException, TransactionAbortedException {
        BTreePageId leftSiblingId = null;
        BTreePageId rightSiblingId = null;
        if (leftEntry != null) {
            leftSiblingId = leftEntry.getLeftChild();
        }
        if (rightEntry != null) {
            rightSiblingId = rightEntry.getRightChild();
        }

        int maxEmptySlots = page.getMaxEntries() - page.getMaxEntries() / 2; // ceiling
        if (leftSiblingId != null) {
            BTreeInternalPage leftSibling =
                    (BTreeInternalPage) getPage(tid, dirtypages, leftSiblingId, Permissions.READ_WRITE);
            // if the left sibling is at minimum occupancy, merge with it. Otherwise
            // steal some entries from it
            if (leftSibling.getNumEmptySlots() >= maxEmptySlots) {
                mergeInternalPages(tid, dirtypages, leftSibling, page, parent, leftEntry);
            } else {
                stealFromLeftInternalPage(tid, dirtypages, page, leftSibling, parent, leftEntry);
            }
        } else if (rightSiblingId != null) {
            BTreeInternalPage rightSibling =
                    (BTreeInternalPage) getPage(tid, dirtypages, rightSiblingId, Permissions.READ_WRITE);
            // if the right sibling is at minimum occupancy, merge with it. Otherwise
            // steal some entries from it
            if (rightSibling.getNumEmptySlots() >= maxEmptySlots) {
                mergeInternalPages(tid, dirtypages, page, rightSibling, parent, rightEntry);
            } else {
                stealFromRightInternalPage(tid, dirtypages, page, rightSibling, parent, rightEntry);
            }
        }
    }

    /**
     * Steal entries from the left sibling and copy them to the given page so that both pages are at least
     * half full. Keys can be thought of as rotating through the parent entry, so the original key in the
     * parent is "pulled down" to the right-hand page, and the last key in the left-hand page is "pushed up"
     * to the parent.  Update parent pointers as needed.
     *
     * @param tid - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param page - the internal page which is less than half full
     * @param leftSibling - the left sibling which has entries to spare
     * @param parent - the parent of the two internal pages
     * @param parentEntry - the entry in the parent pointing to the two internal pages
     * @see #updateParentPointers(TransactionId, HashMap, BTreeInternalPage)
     */
    protected void stealFromLeftInternalPage(TransactionId tid, HashMap<PageId, Page> dirtypages,
            BTreeInternalPage page, BTreeInternalPage leftSibling, BTreeInternalPage parent,
            BTreeEntry parentEntry) throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // Move some of the entries from the left sibling to the page so
        // that the entries are evenly distributed. Be sure to update
        // the corresponding parent entry. Be sure to update the parent
        // pointers of all children in the entries that were moved.
        int numTupleToMove = (leftSibling.getNumEntries() - page.getNumEntries()) / 2;
        Iterator<BTreeEntry> bTreeEntryIterator = leftSibling.reverseIterator();//3 2 1
        int i = 0;
        while (i < numTupleToMove && bTreeEntryIterator.hasNext()){
            BTreeEntry next = bTreeEntryIterator.next();
            leftSibling.deleteKeyAndRightChild(next);

            updateParentPointer(tid,dirtypages, page.getId(),next.getRightChild());
            BTreePageId pid = next.getRightChild();
            next.setLeftChild(parentEntry.getLeftChild());
            next.setRightChild(parentEntry.getRightChild());
            next.setRecordId(parentEntry.getRecordId());
            parent.updateEntry(next);

            parentEntry.setLeftChild(pid);
            parentEntry.setRightChild(page.iterator().next().getLeftChild());
            page.insertEntry(parentEntry);

            parentEntry = next;
            i++;
        }

        dirtypages.put(parent.getId(),parent);
        dirtypages.put(leftSibling.getId(), leftSibling);
        dirtypages.put(page.getId(),page);

        updateParentPointers(tid, dirtypages, page);
        updateParentPointers(tid, dirtypages, leftSibling);

    }

    /**
     * Steal entries from the right sibling and copy them to the given page so that both pages are at least
     * half full. Keys can be thought of as rotating through the parent entry, so the original key in the
     * parent is "pulled down" to the left-hand page, and the last key in the right-hand page is "pushed up"
     * to the parent.  Update parent pointers as needed.
     *
     * @param tid - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param page - the internal page which is less than half full
     * @param rightSibling - the right sibling which has entries to spare
     * @param parent - the parent of the two internal pages
     * @param parentEntry - the entry in the parent pointing to the two internal pages
     * @see #updateParentPointers(TransactionId, HashMap, BTreeInternalPage)
     */
    protected void stealFromRightInternalPage(TransactionId tid, HashMap<PageId, Page> dirtypages,
            BTreeInternalPage page, BTreeInternalPage rightSibling, BTreeInternalPage parent,
            BTreeEntry parentEntry) throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // Move some of the entries from the right sibling to the page so
        // that the entries are evenly distributed. Be sure to update
        // the corresponding parent entry. Be sure to update the parent
        // pointers of all children in the entries that were moved.
        int numTupleToMove = (rightSibling.getNumEntries() - page.getNumEntries()) / 2;
        Iterator<BTreeEntry> bTreeEntryIterator = rightSibling.iterator();//1 2 3
        int i = 0;

        while (i < numTupleToMove && bTreeEntryIterator.hasNext()){
            BTreeEntry next = bTreeEntryIterator.next();
            rightSibling.deleteKeyAndLeftChild(next);

            updateParentPointer(tid,dirtypages, page.getId(),next.getLeftChild());
            BTreePageId pid = next.getLeftChild();
            next.setLeftChild(parentEntry.getLeftChild());
            next.setRightChild(parentEntry.getRightChild());
            next.setRecordId(parentEntry.getRecordId());
            parent.updateEntry(next);

            parentEntry.setRightChild(pid);
            parentEntry.setLeftChild(page.reverseIterator().next().getRightChild());

            page.insertEntry(parentEntry);
            parentEntry = next;
            i++;
        }

        dirtypages.put(parent.getId(),parent);
        dirtypages.put(rightSibling.getId(), rightSibling);
        dirtypages.put(page.getId(),page);

        updateParentPointers(tid, dirtypages, page);
        updateParentPointers(tid, dirtypages, rightSibling);

    }

    /**
     * Merge two leaf pages by moving all tuples from the right page to the left page.
     * Delete the corresponding key and right child pointer from the parent, and recursively
     * handle the case when the parent gets below minimum occupancy.
     * Update sibling pointers as needed, and make the right page available for reuse.
     *
     * @param tid - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param leftPage - the left leaf page
     * @param rightPage - the right leaf page
     * @param parent - the parent of the two pages
     * @param parentEntry - the entry in the parent corresponding to the leftPage and rightPage
     * @see #deleteParentEntry(TransactionId, HashMap, BTreePage, BTreeInternalPage, BTreeEntry)
     */
    /**
     *1。从左边或者右边复制数据
     *2 删除父项的指针 和entry
     * 3 更新指针
     */
    protected void mergeLeafPages(TransactionId tid, HashMap<PageId, Page> dirtypages,
            BTreeLeafPage leftPage, BTreeLeafPage rightPage, BTreeInternalPage parent, BTreeEntry parentEntry)
            throws DbException, IOException, TransactionAbortedException {

        // some code goes here
        //
        // Move all the tuples from the right page to the left page, update
        // the sibling pointers, and make the right page available for reuse.
        // Delete the entry in the parent corresponding to the two pages that are merging -
        // deleteParentEntry() will be useful here

        //合并叶子节点的方法
        //1。从左边或者右边复制数据
        Iterator<Tuple> iterator = rightPage.iterator();
        while(iterator.hasNext()){//1 2 3
            Tuple next = iterator.next();
            rightPage.deleteTuple(next);
            leftPage.insertTuple(next);
        }

        BTreePageId rightSiblingId = rightPage.getRightSiblingId();
        if(rightSiblingId!=null){
            BTreeLeafPage page = (BTreeLeafPage)getPage(tid, dirtypages, rightSiblingId, Permissions.READ_WRITE);
            page.setLeftSiblingId(leftPage.getId());
            dirtypages.put(rightSiblingId,rightPage);
        }
        leftPage.setRightSiblingId(rightSiblingId);
        dirtypages.put(leftPage.getId(),leftPage);

        setEmptyPage(tid,dirtypages,rightPage.getId().getPageNumber());

        deleteParentEntry(tid,dirtypages,leftPage,parent,parentEntry);
    }

    /**
     * Merge two internal pages by moving all entries from the right page to the left page
     * and "pulling down" the corresponding key from the parent entry.
     * Delete the corresponding key and right child pointer from the parent, and recursively
     * handle the case when the parent gets below minimum occupancy.
     * Update parent pointers as needed, and make the right page available for reuse.
     *
     * @param tid - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param leftPage - the left internal page
     * @param rightPage - the right internal page
     * @param parent - the parent of the two pages
     * @param parentEntry - the entry in the parent corresponding to the leftPage and rightPage
     * @see #deleteParentEntry(TransactionId, HashMap, BTreePage, BTreeInternalPage, BTreeEntry)
     * @see #updateParentPointers(TransactionId, HashMap, BTreeInternalPage)
     */
    /**
     * 1
     * 2
     * 3
     */
    protected void mergeInternalPages(TransactionId tid, HashMap<PageId, Page> dirtypages,
            BTreeInternalPage leftPage, BTreeInternalPage rightPage, BTreeInternalPage parent, BTreeEntry parentEntry)
            throws DbException, IOException, TransactionAbortedException {

        // some code goes here
        //
        // Move all the entries from the right page to the left page, update
        // the parent pointers of the children in the entries that were moved,
        // and make the right page available for reuse
        // Delete the entry in the parent corresponding to the two pages that are merging -
        // deleteParentEntry() will be useful here
        //从上到下删除
        deleteParentEntry(tid,dirtypages,leftPage,parent,parentEntry);

        parentEntry.setLeftChild(leftPage.reverseIterator().next().getRightChild());
        parentEntry.setRightChild(rightPage.iterator().next().getLeftChild());

        leftPage.insertEntry(parentEntry);

        //复制数据
        Iterator<BTreeEntry> iterator1 = rightPage.iterator();
        while(iterator1.hasNext()){//1 2 3
            BTreeEntry next = iterator1.next();
            rightPage.deleteKeyAndLeftChild(next);
            updateParentPointer(tid,dirtypages, leftPage.getId(), next.getLeftChild());
            updateParentPointer(tid,dirtypages, leftPage.getId(), next.getRightChild());
            leftPage.insertEntry(next);
        }
        dirtypages.put(leftPage.getId(),leftPage);
        setEmptyPage(tid,dirtypages,rightPage.getId().getPageNumber());
    }

    /**
     * Method to encapsulate the process of deleting an entry (specifically the key and right child)
     * from a parent node.  If the parent becomes empty (no keys remaining), that indicates that it
     * was the root node and should be replaced by its one remaining child.  Otherwise, if it gets
     * below minimum occupancy for non-root internal nodes, it should steal from one of its siblings or
     * merge with a sibling.
     *
     * @param tid - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param leftPage - the child remaining after the key and right child are deleted
     * @param parent - the parent containing the entry to be deleted
     * @param parentEntry - the entry to be deleted
     * @see #handleMinOccupancyPage(TransactionId, HashMap, BTreePage)
     */
    private void deleteParentEntry(TransactionId tid, HashMap<PageId, Page> dirtypages,
            BTreePage leftPage, BTreeInternalPage parent, BTreeEntry parentEntry)
            throws DbException, IOException, TransactionAbortedException {

        // delete the entry in the parent.  If
        // the parent is below minimum occupancy, get some tuples from its siblings
        // or merge with one of the siblings
        parent.deleteKeyAndRightChild(parentEntry);
        int maxEmptySlots = parent.getMaxEntries() - parent.getMaxEntries() / 2; // ceiling
        if (parent.getNumEmptySlots() == parent.getMaxEntries()) {
            // This was the last entry in the parent.
            // In this case, the parent (root node) should be deleted, and the merged
            // page will become the new root
            BTreePageId rootPtrId = parent.getParentId();
            if (rootPtrId.pgcateg() != BTreePageId.ROOT_PTR) {
                throw new DbException("attempting to delete a non-root node");
            }
            BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) getPage(tid, dirtypages, rootPtrId, Permissions.READ_WRITE);
            leftPage.setParentId(rootPtrId);
            rootPtr.setRootId(leftPage.getId());

            // release the parent page for reuse
            setEmptyPage(tid, dirtypages, parent.getId().getPageNumber());
        } else if (parent.getNumEmptySlots() > maxEmptySlots) {
            handleMinOccupancyPage(tid, dirtypages, parent);
        }
    }

    /**
     * Delete a tuple from this BTreeFile.
     * May cause pages to merge or redistribute entries/tuples if the pages
     * become less than half full.
     *
     * @param tid - the transaction id
     * @param t - the tuple to delete
     * @return a list of all pages that were dirtied by this operation. Could include
     * many pages since parent pointers will need to be updated when an internal node merges.
     * @see #handleMinOccupancyPage(TransactionId, HashMap, BTreePage)
     */
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        HashMap<PageId, Page> dirtypages = new HashMap<PageId, Page>();

        BTreePageId pageId = new BTreePageId(tableid, t.getRecordId().getPageId().getPageNumber(),
                BTreePageId.LEAF);
        BTreeLeafPage page = (BTreeLeafPage) getPage(tid, dirtypages, pageId, Permissions.READ_WRITE);
        page.deleteTuple(t);

        // if the page is below minimum occupancy, get some tuples from its siblings
        // or merge with one of the siblings
        int maxEmptySlots = page.getMaxTuples() - page.getMaxTuples() / 2; // ceiling
        if (page.getNumEmptySlots() > maxEmptySlots) {
            handleMinOccupancyPage(tid, dirtypages, page);
        }

        ArrayList<Page> dirtyPagesArr = new ArrayList<Page>();
        dirtyPagesArr.addAll(dirtypages.values());
        return dirtyPagesArr;
    }

    /**
     * Get a read lock on the root pointer page. Create the root pointer page and root page
     * if necessary.
     *
     * @param tid - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @return the root pointer page
     */
    BTreeRootPtrPage getRootPtrPage(TransactionId tid, HashMap<PageId, Page> dirtypages)
            throws DbException, IOException, TransactionAbortedException {
        synchronized (this) {
            if (f.length() == 0) {
                // create the root pointer page and the root page
                BufferedOutputStream bw = new BufferedOutputStream(
                        new FileOutputStream(f, true));
                byte[] emptyRootPtrData = BTreeRootPtrPage.createEmptyPageData();
                byte[] emptyLeafData = BTreeLeafPage.createEmptyPageData();
                bw.write(emptyRootPtrData);
                bw.write(emptyLeafData);
                bw.close();
            }
        }

        // get a read lock on the root pointer page
        return (BTreeRootPtrPage) getPage(tid, dirtypages, BTreeRootPtrPage.getId(tableid), Permissions.READ_ONLY);
    }

    /**
     * Get the page number of the first empty page in this BTreeFile.
     * Creates a new page if none of the existing pages are empty.
     *
     * @param tid - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @return the page number of the first empty page
     */
    protected int getEmptyPageNo(TransactionId tid, HashMap<PageId, Page> dirtypages)
            throws DbException, IOException, TransactionAbortedException {
        // get a read lock on the root pointer page and use it to locate the first header page
        BTreeRootPtrPage rootPtr = getRootPtrPage(tid, dirtypages);
        BTreePageId headerId = rootPtr.getHeaderId();
        int emptyPageNo = 0;

        if (headerId != null) {
            BTreeHeaderPage headerPage = (BTreeHeaderPage) getPage(tid, dirtypages, headerId, Permissions.READ_ONLY);
            int headerPageCount = 0;
            // try to find a header page with an empty slot
            while (headerPage != null && headerPage.getEmptySlot() == -1) {
                headerId = headerPage.getNextPageId();
                if (headerId != null) {
                    headerPage = (BTreeHeaderPage) getPage(tid, dirtypages, headerId, Permissions.READ_ONLY);
                    headerPageCount++;
                } else {
                    headerPage = null;
                }
            }

            // if headerPage is not null, it must have an empty slot
            if (headerPage != null) {
                headerPage = (BTreeHeaderPage) getPage(tid, dirtypages, headerId, Permissions.READ_WRITE);
                int emptySlot = headerPage.getEmptySlot();
                headerPage.markSlotUsed(emptySlot, true);
                emptyPageNo = headerPageCount * BTreeHeaderPage.getNumSlots() + emptySlot;
            }
        }

        // at this point if headerId is null, either there are no header pages
        // or there are no free slots
        if (headerId == null) {
            synchronized (this) {
                // create the new page
                BufferedOutputStream bw = new BufferedOutputStream(
                        new FileOutputStream(f, true));
                byte[] emptyData = BTreeInternalPage.createEmptyPageData();
                bw.write(emptyData);
                bw.close();
                emptyPageNo = numPages();
            }
        }

        return emptyPageNo;
    }

    /**
     * Method to encapsulate the process of creating a new page.  It reuses old pages if possible,
     * and creates a new page if none are available.  It wipes the page on disk and in the cache and
     * returns a clean copy locked with read-write permission
     *
     * @param tid - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param pgcateg - the BTreePageId category of the new page.  Either LEAF, INTERNAL, or HEADER
     * @return the new empty page
     * @see #getEmptyPageNo(TransactionId, HashMap)
     * @see #setEmptyPage(TransactionId, HashMap, int)
     */
    private Page getEmptyPage(TransactionId tid, HashMap<PageId, Page> dirtypages, int pgcateg)
            throws DbException, IOException, TransactionAbortedException {
        // create the new page
        int emptyPageNo = getEmptyPageNo(tid, dirtypages);
        BTreePageId newPageId = new BTreePageId(tableid, emptyPageNo, pgcateg);

        // write empty page to disk
        RandomAccessFile rf = new RandomAccessFile(f, "rw");
        rf.seek(BTreeRootPtrPage.getPageSize() + (emptyPageNo - 1) * BufferPool.getPageSize());
        rf.write(BTreePage.createEmptyPageData());
        rf.close();

        // make sure the page is not in the buffer pool	or in the local cache
        Database.getBufferPool().discardPage(newPageId);
        dirtypages.remove(newPageId);

        return getPage(tid, dirtypages, newPageId, Permissions.READ_WRITE);
    }

    /**
     * Mark a page in this BTreeFile as empty. Find the corresponding header page
     * (create it if needed), and mark the corresponding slot in the header page as empty.
     *
     * @param tid - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param emptyPageNo - the page number of the empty page
     * @see #getEmptyPage(TransactionId, HashMap, int)
     */
    protected void setEmptyPage(TransactionId tid, HashMap<PageId, Page> dirtypages, int emptyPageNo)
            throws DbException, IOException, TransactionAbortedException {

        // if this is the last page in the file (and not the only page), just
        // truncate the file
        // @TODO: Commented out because we should probably do this somewhere else in case the transaction aborts....
        //		synchronized(this) {
        //			if(emptyPageNo == numPages()) {
        //				if(emptyPageNo <= 1) {
        //					// if this is the only page in the file, just return.
        //					// It just means we have an empty root page
        //					return;
        //				}
        //				long newSize = f.length() - BufferPool.getPageSize();
        //				FileOutputStream fos = new FileOutputStream(f, true);
        //				FileChannel fc = fos.getChannel();
        //				fc.truncate(newSize);
        //				fc.close();
        //				fos.close();
        //				return;
        //			}
        //		}

        // otherwise, get a read lock on the root pointer page and use it to locate
        // the first header page
        BTreeRootPtrPage rootPtr = getRootPtrPage(tid, dirtypages);
        BTreePageId headerId = rootPtr.getHeaderId();
        BTreePageId prevId = null;
        int headerPageCount = 0;

        // if there are no header pages, create the first header page and update
        // the header pointer in the BTreeRootPtrPage
        if (headerId == null) {
            rootPtr = (BTreeRootPtrPage) getPage(tid, dirtypages, BTreeRootPtrPage.getId(tableid),
                    Permissions.READ_WRITE);

            BTreeHeaderPage headerPage = (BTreeHeaderPage) getEmptyPage(tid, dirtypages, BTreePageId.HEADER);
            headerId = headerPage.getId();
            headerPage.init();
            rootPtr.setHeaderId(headerId);
        }

        // iterate through all the existing header pages to find the one containing the slot
        // corresponding to emptyPageNo
        while (headerId != null && (headerPageCount + 1) * BTreeHeaderPage.getNumSlots() < emptyPageNo) {
            BTreeHeaderPage headerPage = (BTreeHeaderPage) getPage(tid, dirtypages, headerId, Permissions.READ_ONLY);
            prevId = headerId;
            headerId = headerPage.getNextPageId();
            headerPageCount++;
        }

        // at this point headerId should either be null or set with
        // the headerPage containing the slot corresponding to emptyPageNo.
        // Add header pages until we have one with a slot corresponding to emptyPageNo
        while ((headerPageCount + 1) * BTreeHeaderPage.getNumSlots() < emptyPageNo) {
            BTreeHeaderPage prevPage = (BTreeHeaderPage) getPage(tid, dirtypages, prevId, Permissions.READ_WRITE);

            BTreeHeaderPage headerPage = (BTreeHeaderPage) getEmptyPage(tid, dirtypages, BTreePageId.HEADER);
            headerId = headerPage.getId();
            headerPage.init();
            headerPage.setPrevPageId(prevId);
            prevPage.setNextPageId(headerId);

            headerPageCount++;
            prevId = headerId;
        }

        // now headerId should be set with the headerPage containing the slot corresponding to
        // emptyPageNo
        BTreeHeaderPage headerPage = (BTreeHeaderPage) getPage(tid, dirtypages, headerId, Permissions.READ_WRITE);
        int emptySlot = emptyPageNo - headerPageCount * BTreeHeaderPage.getNumSlots();
        headerPage.markSlotUsed(emptySlot, false);
    }

    /**
     * get the specified tuples from the file based on its IndexPredicate value on
     * behalf of the specified transaction. This method will acquire a read lock on
     * the affected pages of the file, and may block until the lock can be
     * acquired.
     *
     * @param tid - the transaction id
     * @param ipred - the index predicate value to filter on
     * @return an iterator for the filtered tuples
     */
    public DbFileIterator indexIterator(TransactionId tid, IndexPredicate ipred) {
        return new BTreeSearchIterator(this, tid, ipred);
    }

    /**
     * Get an iterator for all tuples in this B+ tree file in sorted order. This method
     * will acquire a read lock on the affected pages of the file, and may block until
     * the lock can be acquired.
     *
     * @param tid - the transaction id
     * @return an iterator for all the tuples in this file
     */
    public DbFileIterator iterator(TransactionId tid) {
        return new BTreeFileIterator(this, tid);
    }

}

/**
 * Helper class that implements the Java Iterator for tuples on a BTreeFile
 */
class BTreeFileIterator extends AbstractDbFileIterator {

    Iterator<Tuple> it = null;
    BTreeLeafPage curp = null;

    TransactionId tid;
    BTreeFile f;

    /**
     * Constructor for this iterator
     *
     * @param f - the BTreeFile containing the tuples
     * @param tid - the transaction id
     */
    public BTreeFileIterator(BTreeFile f, TransactionId tid) {
        this.f = f;
        this.tid = tid;
    }

    /**
     * Open this iterator by getting an iterator on the first leaf page
     */
    public void open() throws DbException, TransactionAbortedException {
        BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) Database.getBufferPool().getPage(
                tid, BTreeRootPtrPage.getId(f.getId()), Permissions.READ_ONLY);
        BTreePageId root = rootPtr.getRootId();
        curp = f.findLeafPage(tid, root, Permissions.READ_ONLY, null);
        it = curp.iterator();
    }

    /**
     * Read the next tuple either from the current page if it has more tuples or
     * from the next page by following the right sibling pointer.
     *
     * @return the next tuple, or null if none exists
     */
    @Override
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        if (it != null && !it.hasNext()) {
            it = null;
        }

        while (it == null && curp != null) {
            BTreePageId nextp = curp.getRightSiblingId();
            if (nextp == null) {
                curp = null;
            } else {
                curp = (BTreeLeafPage) Database.getBufferPool().getPage(tid,
                        nextp, Permissions.READ_ONLY);
                it = curp.iterator();
                if (!it.hasNext()) {
                    it = null;
                }
            }
        }

        if (it == null) {
            return null;
        }
        return it.next();
    }

    /**
     * rewind this iterator back to the beginning of the tuples
     */
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    /**
     * close the iterator
     */
    public void close() {
        super.close();
        it = null;
        curp = null;
    }
}

/**
 * Helper class that implements the DbFileIterator for search tuples on a
 * B+ Tree File
 */
class BTreeSearchIterator extends AbstractDbFileIterator {

    Iterator<Tuple> it = null;
    BTreeLeafPage curp = null;

    TransactionId tid;
    BTreeFile f;
    IndexPredicate ipred;

    /**
     * Constructor for this iterator
     *
     * @param f - the BTreeFile containing the tuples
     * @param tid - the transaction id
     * @param ipred - the predicate to filter on
     */
    public BTreeSearchIterator(BTreeFile f, TransactionId tid, IndexPredicate ipred) {
        this.f = f;
        this.tid = tid;
        this.ipred = ipred;
    }

    /**
     * Open this iterator by getting an iterator on the first leaf page applicable
     * for the given predicate operation
     */
    public void open() throws DbException, TransactionAbortedException {
        BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) Database.getBufferPool().getPage(
                tid, BTreeRootPtrPage.getId(f.getId()), Permissions.READ_ONLY);
        BTreePageId root = rootPtr.getRootId();
        if (ipred.getOp() == Op.EQUALS || ipred.getOp() == Op.GREATER_THAN
                || ipred.getOp() == Op.GREATER_THAN_OR_EQ) {
            curp = f.findLeafPage(tid, root, Permissions.READ_ONLY, ipred.getField());
        } else {
            curp = f.findLeafPage(tid, root, Permissions.READ_ONLY, null);
        }
        it = curp.iterator();
    }

    /**
     * Read the next tuple either from the current page if it has more tuples matching
     * the predicate or from the next page by following the right sibling pointer.
     *
     * @return the next tuple matching the predicate, or null if none exists
     */
    @Override
    protected Tuple readNext() throws TransactionAbortedException, DbException,
            NoSuchElementException {
        while (it != null) {

            while (it.hasNext()) {
                Tuple t = it.next();
                if (t.getField(f.keyField()).compare(ipred.getOp(), ipred.getField())) {
                    return t;
                } else if (ipred.getOp() == Op.LESS_THAN || ipred.getOp() == Op.LESS_THAN_OR_EQ) {
                    // if the predicate was not satisfied and the operation is less than, we have
                    // hit the end
                    return null;
                } else if (ipred.getOp() == Op.EQUALS &&
                        t.getField(f.keyField()).compare(Op.GREATER_THAN, ipred.getField())) {
                    // if the tuple is now greater than the field passed in and the operation
                    // is equals, we have reached the end
                    return null;
                }
            }

            BTreePageId nextp = curp.getRightSiblingId();
            // if there are no more pages to the right, end the iteration
            if (nextp == null) {
                return null;
            } else {
                curp = (BTreeLeafPage) Database.getBufferPool().getPage(tid,
                        nextp, Permissions.READ_ONLY);
                it = curp.iterator();
            }
        }

        return null;
    }

    /**
     * rewind this iterator back to the beginning of the tuples
     */
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    /**
     * close the iterator
     */
    public void close() {
        super.close();
        it = null;
    }
}
