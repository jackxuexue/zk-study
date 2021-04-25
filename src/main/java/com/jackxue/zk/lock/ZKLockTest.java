package com.jackxue.zk.lock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 测试第一种zookeeper的锁，谁创建了锁的znode就谁获取锁。
 * 会出现惊群的现象
 */
public class ZKLockTest {
    private Logger logger  = LoggerFactory.getLogger(ZKLockTest.class);

    public static void main(String[] args) {
        ZKLock zkLock = new ZKLock();
        zkLock.lock();
        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                zkLock.lock();
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                zkLock.unlock();
            }
        }, "t1");
        t1.start();
        try {
            TimeUnit.SECONDS.sleep(2);
            zkLock.unlock();
            t1.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}

interface lock{
    void lock();
    void unlock();
}

class ZKLock implements lock, Watcher {
    final static Logger logger = LoggerFactory.getLogger(ZKLock.class);

    private ZooKeeper zk;
    private String lockName = "/lock";
    private String nodeName = "/lock/test";

    public ZKLock() {
        try {
            logger.error("hello world!");
            zk = new ZooKeeper("118.89.203.187", 5000, this);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void lock() {
        if(zk==null){
            return;
        }
        Stat stat;
        //1.判断根节点是否存在，如果不存在则创建根阶段和子节点，并且获取到锁
        try {
            if ((stat = zk.exists(lockName, false)) == null) {
                //2.创建根节点为临时节点
                String s = null;
                try {
                    s = zk.create(lockName, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    System.out.println(s);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }finally {
                    s = zk.create(nodeName, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                    System.out.println(s);
                    //获取到锁
                    if(s != null){
                        System.out.println(Thread.currentThread().getName() + " 获得锁！");
                        logger.error("获得锁！ ");
                        return;
                    }
                }

            }else {
                while (true) {
                    try {
                        String path = zk.create(nodeName, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                        if (path != null) {
                            logger.error("获得锁！ ");
                            System.out.println(Thread.currentThread().getName() + " 获得锁！");
                            return;
                        }
                    }catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (KeeperException e) {
                        CountDownLatch countDownLatch = new CountDownLatch(1);
                        zk.exists(nodeName, new Watcher() {
                            @Override
                            public void process(WatchedEvent event) {
                                countDownLatch.countDown();;
                            }
                        });
                        System.out.println(Thread.currentThread().getName() + " 等待获取锁...");
                        countDownLatch.await();
                    }
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void unlock() {
        try {
            System.out.println(Thread.currentThread().getName() + " 释放锁");
            logger.error("释放锁");
            zk.delete(nodeName, -1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void process(WatchedEvent event) {
        if(event.getState().equals(Event.KeeperState.SyncConnected)){
            if(event.getType().equals(Event.EventType.NodeChildrenChanged)){
                System.out.println(event.toString());
            }
        }
    }
}
