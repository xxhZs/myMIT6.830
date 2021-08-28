package simpledb;

import org.junit.Test;

/**
 * @author yourname <xuxinhao@kuaishou.com>
 * Created on 2021-08-22
 */
//public class LockManagerTest {
//    static int a =0;
//    public static void main(String[] args) {
//
//        LockManager lockManager = new LockManager();
//        TransactionId t1= new TransactionId();
//        TransactionId t2 = new TransactionId();
//        PageId pageId1 =new HeapPageId(1,1);
//        Runnable runnable1 = new Runnable() {
//            @Override
//            public void run() {
//                try {
//                    System.out.println("事务1写"+Thread.currentThread());
//                    lockManager.acquireLock(t1,pageId1, LockManager.WriteOrRead.WRITE);
//                    System.out.println("事务1写锁加上了"+Thread.currentThread());
//                    Thread.sleep(1000);
//                    System.out.println("事务1写锁准备释放"+Thread.currentThread());
//                    lockManager.releaseLock(t1,pageId1, LockManager.WriteOrRead.WRITE);
//                    System.out.println("事务1写锁释放"+Thread.currentThread());
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
//        };
//        Runnable runnable2 = new Runnable() {
//            @Override
//            public void run() {
//                try {
//                    System.out.println("事务2读"+Thread.currentThread());
//                    lockManager.acquireLock(t2,pageId1, LockManager.WriteOrRead.READ);
//                    System.out.println("事务2读锁加上了"+Thread.currentThread());
////                    Thread.sleep(1000);
//                    System.out.println(a);
//                    System.out.println("事务2读锁准备释放"+Thread.currentThread());
//                    lockManager.releaseLock(t2,pageId1, LockManager.WriteOrRead.READ);
//                    System.out.println("事务2读锁释放"+Thread.currentThread());
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
//        };
//
//        for(int i =0 ;i<10;i++){
//            TransactionId t3= new TransactionId();
//            TransactionId t4= new TransactionId();
//            Runnable runnable3 = new Runnable() {
//                @Override
//                public void run() {
//                    try {
//                        System.out.println(t3.toString()+"写"+Thread.currentThread());
//                        lockManager.acquireLock(t3,pageId1, LockManager.WriteOrRead.WRITE);
//                        System.out.println(t3.toString()+"写锁加上了"+Thread.currentThread());
//                        a=a+1;
//                        System.out.println(a+"写完");
//                        System.out.println(t3.toString()+"写锁准备释放"+Thread.currentThread());
//                        lockManager.releaseLock(t3,pageId1, LockManager.WriteOrRead.WRITE);
//                        System.out.println(t3.toString()+"写锁释放"+Thread.currentThread());
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                }
//            };
//            Runnable runnable4 = new Runnable() {
//                @Override
//                public void run() {
//                    try {
//                        Thread.sleep(5);
//                        System.out.println(t4.toString()+"读"+Thread.currentThread());
//                        lockManager.acquireLock(t4,pageId1, LockManager.WriteOrRead.READ);
//                        System.out.println(t4.toString()+"读锁加上了"+Thread.currentThread());
//                        System.out.println(a);
//                        System.out.println(t4.toString()+"读锁准备释放"+Thread.currentThread());
//                        lockManager.releaseLock(t4,pageId1, LockManager.WriteOrRead.READ);
//                        System.out.println(t4.toString()+"读锁释放"+Thread.currentThread());
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                }
//            };
//            new Thread(runnable3).start();
//            new Thread(runnable4).start();
//        }
//    }
//
//}

//    @Test
//    public void testReadWrite(){
//        Runnable runnable1 = new Runnable() {
//            @Override
//            public void run() {
//                try {
//                    System.out.println("事务1写");
//                    lockManager.acquireLock(t1,pageId1, LockManager.WriteOrRead.WRITE);
//                    System.out.println("事务1写锁加上了");
//                    Thread.sleep(1000);
//                    System.out.println("事务1写锁准备释放");
//                    lockManager.releaseLock(t1,pageId1, LockManager.WriteOrRead.WRITE);
//                    System.out.println("事务1写锁释放");
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
//        };
//        Runnable runnable2 = new Runnable() {
//            @Override
//            public void run() {
//                try {
//                    System.out.println("事务2读");
//                    lockManager.acquireLock(t2,pageId1, LockManager.WriteOrRead.READ);
//                    System.out.println("事务2读锁加上了");
//                    Thread.sleep(1000);
//                    System.out.println("事务2读锁准备释放");
//                    lockManager.releaseLock(t2,pageId1, LockManager.WriteOrRead.READ);
//                    System.out.println("事务2读锁释放");
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
//        };
//        Thread thread = new Thread(runnable1);
//        Thread thread1 = new Thread(runnable2);
//        thread.start();
//        thread1.start();
//    }
//    @Test
//    public void testReadWrite1(){
//        Runnable runnable1 = new Runnable() {
//            @Override
//            public void run() {
//                try {
//                    System.out.println("事务1读");
//                    lockManager.acquireLock(t1,pageId1, LockManager.WriteOrRead.READ);
//                    System.out.println("事务1读锁加上了");
//                    Thread.sleep(3000);
//                    System.out.println("事务1读锁准备释放");
//                    lockManager.releaseLock(t1,pageId1, LockManager.WriteOrRead.READ);
//                    System.out.println("事务1读锁释放");
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
//        };
//        Runnable runnable2 = new Runnable() {
//            @Override
//            public void run() {
//                try {
//                    System.out.println("事务2读");
//                    lockManager.acquireLock(t2,pageId1, LockManager.WriteOrRead.READ);
//                    System.out.println("事务2读锁加上了");
//                    Thread.sleep(3000);
//                    System.out.println("事务2读锁准备释放");
//                    lockManager.releaseLock(t2,pageId1, LockManager.WriteOrRead.READ);
//                    System.out.println("事务2读锁释放");
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
//        };
//        Thread thread = new Thread(runnable1);
//        Thread thread1 = new Thread(runnable2);
//        thread.start();
//        thread1.start();
//    }
//}
