package com.dw.zk;

import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

/**
 * Description: this is from the origin project-menagerie
 * @author caohui
 */
public class ZkPrimitive {

	// Represents an empty znode (no data on the node)
	protected static final byte[] EMPTYNODE = new byte[]{};
	 
	// The Session Manager to use with this Primitive.
	protected final ZkSessionManager zkSessionManager;
	
	// the base node for all behaviors
	protected final String baseNode;
	
	// The ACL privileges for this Primitive to use
	protected final List<ACL> privileges;
	
	// A local mutex lock for managing inter-thread synchronization
	protected final Lock localLock;
	
	// A local mutex condition
	protected final Condition condition;
	
	// Connection listener to attach to the session manager when listening for Session events is necessary
	protected final ConnectionListener connectionListener = new PrimitiveConnectionListener(this);
	
	// A signalling wathcer, whose job it is to call (java.util.concurrent.locks.Condition#signal() or
	// java.util.concurrent.locks.Condition#signalAll()) to notify any threads sleeping through the
	// local instance.
	protected final Watcher signalWatcher;
	
	/**
	 * Description: Creates a new ZkPrimitive with the correct node information.
	 * All Rights Reserved.
	 *
	 * @param baseNode             the base node to use
	 * @param zkSessionmanager     the session manager to use
	 * @param privileges           the privileges for this node.
	 * @return void
	 * @version 1.0 2016年12月27日 下午1:48:57 created by caohui(1343965426@qq.com)
	 */
	protected ZkPrimitive(String baseNode, ZkSessionManager zkSessionmanager, List<ACL> privileges) {
		if(baseNode == null) {
			throw new NullPointerException("No base node specified!");
		}
		
		this.baseNode = baseNode;
		this.zkSessionManager = zkSessionmanager;
		this.privileges = privileges;
		
		this.localLock = new ReentrantLock(true);
		this.condition = this.localLock.newCondition();
		this.signalWatcher = new SignallingWatcher(this);
		ensureNodeExists();
	}
	
	protected final void ensureNodeExists() {
		try {
			ZooKeeper zooKeeper = zkSessionManager.getZooKeeper();
			Stat stat = zooKeeper.exists(baseNode, false);
			if(stat == null) {
				 ZkUtils.notExitCreate(zkSessionManager, baseNode);
				// zooKeeper.create(baseNode, EMPTYNODE, privileges, CreateMode.PERSISTENT);
			}
		}catch(KeeperException e) {
			// if the node already exists, then we are happy, so ignore those exceptions
			if(e.code() != KeeperException.Code.NODEEXISTS) {
				throw new RuntimeException(e);
			}
		}catch(InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
	
	
	@Override
	public boolean equals(Object o) {
		if(o == null) {
			return false;
		}
		
		if(this == o) {
			return true;
		}
		
		if(o.getClass() != this.getClass()) {
			return false;
		}
		
		ZkPrimitive that = (ZkPrimitive) o;
		return baseNode.equals(that.baseNode);
	}
	
	@Override
	public int hashCode() {
		return baseNode.hashCode();
	}


	// Set to the Session Manager to begin listening for session events.
	protected void setConnectionListener() {
		zkSessionManager.addConnectionListener(connectionListener);
	}


	// remove from the Session Manager, and stop listening for session event.
	protected void removeConnectionListener() {
		zkSessionManager.removeConnectionListener(connectionListener);
	}

	// Notifies any/all parties which may be waiting for to fire.
	protected void notifyParties() {
		localLock.lock();
		try {
			condition.signalAll();
		}finally {
			localLock.unlock();
		}
	}
	private static final class PrimitiveConnectionListener extends ConnectionListenerSkeleton {
		private final ZkPrimitive primitive;
		
		private PrimitiveConnectionListener(ZkPrimitive primitive) {
			this.primitive = primitive;
		}

		@Override
		public void syncConnected() {
			// We had to connect to another server, and this way have taken time, causing us to miss our watcher, so let's
			// signal everyone locally and see what we get.
			primitive.notifyParties();
		}

		@Override
		public void expired() {
			// indicate that this lock is broken, and alert all waiting threads to throw an Exception
			primitive.notifyParties();
		}
	}
	
	
	private static final class SignallingWatcher implements Watcher {
		private final ZkPrimitive primitive;
		
		private SignallingWatcher(ZkPrimitive primitive) {
			this.primitive = primitive;
		}

		@Override
		public void process(WatchedEvent event) {
			primitive.notifyParties();
		}
	}
}
