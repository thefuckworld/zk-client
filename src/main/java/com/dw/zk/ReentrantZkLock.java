package com.dw.zk;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dw.util.LocalUtils;

public class ReentrantZkLock extends ZkPrimitive implements Lock{

	// A default delimiter to separate a lockPrefix from the sequential elements set by ZooKeeper
	protected static final char LOCKDELIMITER = '-';
	private static final String LOCKPREFIX = "lock";
	
	// 当前地址存储在zk中
	private static final String DEFAULTVALUE = LocalUtils.getLocalIp();
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ReentrantZkLock.class);
	
	protected final ThreadLocal<LockHolder> locks = new ThreadLocal<LockHolder> ();
	
	/**
	 * Description: Constructs a new Lock on the specified node, using Open ACL privilegs.
	 * All Rights Reserved.
	 *
	 * @param baseNode
	 * @param zkSessionManager
	 *
	 * @version 1.0 2016年12月27日 下午2:31:12 created by caohui(1343965426@qq.com)
	 */
	public ReentrantZkLock(String baseNode, ZkSessionManager zkSessionManager) {
		super(baseNode, zkSessionManager, ZooDefs.Ids.OPEN_ACL_UNSAFE);
	}
	
	public ReentrantZkLock(String baseNode, ZkSessionManager zkSessionManager, List<ACL> privilegs) {
		super(baseNode, zkSessionManager, privilegs);
	}
	// Holder for information about a specific lock
	static final class LockHolder {
		private final String lockNode;
		private final AtomicInteger numLocks = new AtomicInteger(1);
		
		private LockHolder(String lockNode) {
			this.lockNode = lockNode;
		}
		
		public void incrementLock() {
			numLocks.incrementAndGet();
		}
		
		public int decrementLock() {
			return numLocks.decrementAndGet();
		}
		
		public String lockNode() {
			return lockNode;
		}
	}
	
	
	@Override
	public void lock() {
		if(checkReentrancy()) {
			return;
		}
		// set a connection listener to listener for session expiration
		setConnectionListener();
		
		String lockNode = null;
		try {
			while(true) {
				localLock.lock();
				if(StringUtils.isEmpty(lockNode)) {
					lockNode = createNode();
				}
				
				try {
					// ask ZooKeeper for the lock
					boolean acquiredLock = tryAcquireDistributed(zkSessionManager.getZooKeeper(), lockNode, true);
					if(!acquiredLock) {
						// we don't have the lock, so we need to wait for our wathcer to fire
						// this method is not interruptible, so need to wait appropriately
						condition.awaitUninterruptibly();
					}else {
						// we have the lock, so return happy
						// 设置当前线程可重入当前锁
						LockHolder holder = locks.get();
						if(holder != null) {
							holder.incrementLock();
						}else {
							locks.set(new LockHolder(lockNode));
						}
					}
				}finally {
					localLock.unlock();
				}
			}
		}catch(Exception e) {
			LOGGER.error(e.getMessage(), e);
			if(!StringUtils.isEmpty(lockNode)) {
				try {
					ZkUtils.ensureDelete(zkSessionManager, lockNode, -1);
				}catch(InterruptedException e1) {
					LOGGER.error(e.getMessage(), e1);
				}
			}
			throw new RuntimeException(e);
		}finally {
			// we no longer care about having a ConnectionListener here
			removeConnectionListener();
		}
	}
	
	protected boolean tryAcquireDistributed(ZooKeeper zk, String lockNode, boolean watch) throws KeeperException, InterruptedException {
		List<String> tempLocks = ZkInternalUtils.filterByPrefix(zk.getChildren(baseNode, false), getLockPrefix());
		ZkInternalUtils.sortBySequence(tempLocks, LOCKDELIMITER);
		
		String myNodeName = lockNode.substring(lockNode.lastIndexOf('/') + 1);
		int myPos = tempLocks.indexOf(myNodeName);
		
		int nextNodePos = myPos - 1;
		while(nextNodePos >= 0) {
			Stat stat = null;
			if(watch) {
				stat = zk.exists(baseNode + "/" + tempLocks.get(nextNodePos), signalWatcher);
			}else {
				stat = zk.exists(baseNode + "/" + tempLocks.get(nextNodePos), false);
			}
			
			if(stat != null) {
				// there is a node which already has the lock, so we need wait for notifying that
				return false;
			}
			
			nextNodePos--;
		}
		
		return true;
	}
	
	private String createNode() {
		byte[] data = EMPTYNODE;
		try {
			data = DEFAULTVALUE.getBytes("utf-8");
		}catch(UnsupportedEncodingException e) {
			LOGGER.error("", e);
		}
		return ZkUtils.ensureCreate(ZkUtils.ZK_SESSION_MANAGER, getBaseLockPath(), data, privileges, CreateMode.EPHEMERAL_SEQUENTIAL);
	}
	
	protected String getBaseLockPath() {
		return baseNode + "/" + getLockPrefix() + LOCKDELIMITER;
	}
	
	protected String getLockPrefix() {
		return LOCKPREFIX;
	}
	
	/*
	 * Checks whether or not this party is re-entering a lock which it already owns.
	 * If this party already owns the lock, this method increments the lock counter and returns true.
	 * Otherwise, it return false.
	 */
	private boolean checkReentrancy() {
		LockHolder local = locks.get();
		if(local != null) {
			local.incrementLock();
			return true;
		}
		return false;
	}

	@Override
	public void lockInterruptibly() throws InterruptedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean tryLock() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean tryLock(long time, TimeUnit unit)
			throws InterruptedException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void unlock() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Condition newCondition() {
		// TODO Auto-generated method stub
		return null;
	}

}
