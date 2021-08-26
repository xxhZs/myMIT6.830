package simpledb;

/**
 * @author yourname <xuxinhao@kuaishou.com>
 * Created on 2021-08-22
 */

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 一个页级锁管理类
 */
public class LockManager {
    enum WriteOrRead{
        WRITE,READ
    }
    class PageWriteOrRead{
        private PageId pageId;
        private WriteOrRead writeOrRead;
        public PageWriteOrRead(PageId pageId,WriteOrRead writeOrRead){
            this.pageId = pageId;
            this.writeOrRead = writeOrRead;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PageWriteOrRead that = (PageWriteOrRead) o;
            return Objects.equals(pageId, that.pageId) && Objects.equals(writeOrRead, that.writeOrRead);
        }

        @Override
        public int hashCode() {
            return Objects.hash(pageId, writeOrRead);
        }
    }
    private ConcurrentHashMap<PageWriteOrRead, ArrayList<TransactionId>> lockMap;


    public LockManager(){
        this.lockMap = new ConcurrentHashMap<>();
    }
    public synchronized void acquireLock(TransactionId tid,PageId pid,WriteOrRead writeOrRead) throws InterruptedException {
        if(writeOrRead.equals(WriteOrRead.WRITE)){
            while(true){
                if(getWriteLock(tid,pid)){
                    break;
                }
            }
        }else{
            while(true){
                if(getReadLock(tid,pid)){
                    break;
                }
            }
        }
    }
    public boolean getReadLock(TransactionId tid,PageId pid) throws InterruptedException {
        PageWriteOrRead readLockPage  = new PageWriteOrRead(pid, WriteOrRead.READ);
        PageWriteOrRead writeLockPage  = new PageWriteOrRead(pid, WriteOrRead.WRITE);
        synchronized (this){
            if(lockMap.containsKey(writeLockPage)){
                if(lockMap.get(writeLockPage).get(0).equals(tid)){
                    return true;
                }else{
                    wait();
                    return false;
                }
            }else if(lockMap.containsKey(readLockPage)){
                if(lockMap.get(readLockPage).contains(tid)){
                    return true;
                }else{
                    ArrayList<TransactionId> transactionIds = lockMap.get(readLockPage);
                    transactionIds.add(tid);
                    lockMap.put(readLockPage,transactionIds);
                    return true;
                }
            }else{
                ArrayList<TransactionId> objects = new ArrayList<>();
                objects.add(tid);
                lockMap.put(readLockPage,objects);
                return true;
            }
        }
    }
    public boolean getWriteLock(TransactionId tid,PageId pid) throws InterruptedException {
        PageWriteOrRead readLockPage  = new PageWriteOrRead(pid, WriteOrRead.READ);
        PageWriteOrRead writeLockPage  = new PageWriteOrRead(pid, WriteOrRead.WRITE);
        synchronized (this){
            if(lockMap.containsKey(writeLockPage)){
                if(lockMap.get(writeLockPage).get(0).equals(tid)){
                   return true;
                }else{
                    wait();
                    return false;
                }
            }else if(lockMap.containsKey(readLockPage)){
                if(lockMap.get(readLockPage).contains(tid)){
                    return true;
                }else{
                    wait();
                    return false;
                }
            }else{
                ArrayList<TransactionId> objects = new ArrayList<>();
                objects.add(tid);
                lockMap.put(writeLockPage,objects);
                return true;
            }
        }
    }

    public synchronized void releaseLock(TransactionId transactionId,PageId pageId,WriteOrRead writeOrRead){
        PageWriteOrRead pageWriteOrRead = new PageWriteOrRead(pageId,writeOrRead);
        ArrayList<TransactionId> transactionIds = lockMap.get(pageWriteOrRead);
        if(transactionIds==null){
            return;
        }
        if(transactionIds.size()==1){
            lockMap.remove(pageWriteOrRead);
        }else{
            transactionIds.remove(transactionId);
            lockMap.put(pageWriteOrRead,transactionIds);
        }
        notifyAll();
    }
    public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
        PageWriteOrRead readLockPage  = new PageWriteOrRead(pid, WriteOrRead.READ);
        PageWriteOrRead writeLockPage  = new PageWriteOrRead(pid, WriteOrRead.WRITE);
        return (lockMap.containsKey(readLockPage)&&lockMap.get(readLockPage).contains(tid))
                ||(lockMap.containsKey(writeLockPage)&&lockMap.get(writeLockPage).contains(tid));
    }
}
