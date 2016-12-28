package com.dw.zk;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

/**
 * Description: manipulating ZooKeeper-related strings.
 * @author caohui
 */
final class ZkInternalUtils {
	
	private ZkInternalUtils() {}
	
	public static void sortBySequence(List<String> items, char sequenceDelimiter) {
		Collections.sort(items, new SequenceComparator(sequenceDelimiter));
	}
	
	public static void sortByReverseSequence(List<String> items, char sequenceDelimiter) {
		Collections.sort(items, Collections.reverseOrder(new SequenceComparator(sequenceDelimiter)));
	}

	public static int parseSequenceNumber(String node, char sequenceStartDelimiter) {
		String sequenceStr = parseSequenceString(node, sequenceStartDelimiter);
		return Integer.parseInt(sequenceStr);
	}
	
	public static String parseSequenceString(String node, char sequenceStartDelimiter) {
		if(node == null) {
			throw new NullPointerException("No node specified!");
		}
		
		int seqStartIndex = node.lastIndexOf(sequenceStartDelimiter);
		if(seqStartIndex < 0) {
			throw new IllegalArgumentException("No sequence is parseable from the specified node:" +
		        "Node=<"+ node +">, sequence delimiter=<"+ sequenceStartDelimiter +">");
		}
		
		return node.substring(seqStartIndex + 1);
	}
	
	
	public static List<String> filterByPrefix(List<String> nodes, String ... prefixes) {
		List<String> lockChildren = new ArrayList<String> ();
		for(String child : nodes) {
			for(String prefix : prefixes) {
				if(child.startsWith(prefix)) {
					lockChildren.add(child);
					break;
				}
			}
		}
		return lockChildren;
	}
	
	
	public static boolean safeDelete(ZooKeeper zk, String nodeToDelete, int version) throws KeeperException, InterruptedException {
		try {
			zk.delete(nodeToDelete, version);
			return true;
		}catch(KeeperException e) {
			// if the node has already bean deleted, don't worry about it
			if(e.code() != KeeperException.Code.NONODE) {
				throw e;
			}
			return false;
		}
	}
	
	public static boolean uninterruptibleSafeDelete(ZooKeeper zk, String nodeToDelete, int version) throws KeeperException {
		try {
			zk.delete(nodeToDelete, version);
			return true;
		}catch(KeeperException e) {
			if(e.code() != KeeperException.Code.NONODE) {
				throw e;
			}
			return false;
		}catch(InterruptedException e) {
			Thread.currentThread().interrupt();
			return false;
		}
	}
	
	/**
	 * Description: Deletes all the listed elements from ZooKeeper safely.
	 * All Rights Reserved.
	 *
	 * @param zk
	 * @param version
	 * @param nodesToDelete
	 * @throws KeeperException
	 * @throws InterruptedException
	 * @return void
	 * @version 1.0 2016年12月27日 下午10:17:43 created by caohui(1343965426@qq.com)
	 */
	public static void safeDeleteAll(ZooKeeper zk, int version, String ... nodesToDelete) throws KeeperException, InterruptedException {
		for(String permitNode : nodesToDelete) {
			ZkInternalUtils.safeDelete(zk, permitNode, version);
		}
	}
	
	/**
	 * Description: Delete all the listed elements from ZooKeeper safely.
	 * All Rights Reserved.
	 *
	 * @param zk
	 * @param nodeToDelete
	 * @param version
	 * @throws KeeperException
	 * @throws InterruptedException
	 * @return void
	 * @version 1.0 2016年12月27日 下午10:22:12 created by caohui(1343965426@qq.com)
	 */
	public static void recursiveSafeDelete(ZooKeeper zk, String nodeToDelete, int version) throws KeeperException, InterruptedException {
		try {
			List<String> children = zk.getChildren(nodeToDelete, false);
			for(String child : children) {
				recursiveSafeDelete(zk, nodeToDelete + "/" + child, version);
			}
			// delete this node
			safeDelete(zk, nodeToDelete, version);
		}catch(KeeperException e) {
			if(e.code() != KeeperException.Code.NONODE) {
				throw e;
			}
		}
	}
	
	/**
	 * Description: Creates a new node safely.
	 * All Rights Reserved.
	 *
	 * @param zk
	 * @param nodeToCreate
	 * @param data
	 * @param privileges
	 * @param createMode
	 * @return
	 * @throws KeeperException
	 * @throws InterruptedException
	 * @return String
	 * @version 1.0 2016年12月27日 下午10:26:26 created by caohui(1343965426@qq.com)
	 */
	public static String safeCreate(ZooKeeper zk, String nodeToCreate, byte[] data, List<ACL> privileges, CreateMode createMode) throws KeeperException, InterruptedException {
		try {
			return zk.create(nodeToCreate, data, privileges, createMode);
		}catch(KeeperException e) {
			// if the node has already been created, don't worry about it
			if(e.code() != KeeperException.Code.NODEEXISTS) {
				throw e;
			}
			
			return nodeToCreate;
		}
	}
	
	
	/**
	 * Description: Attempts to get data from ZooKeeper in a single operation.
	 * All Rights Reserved.
	 *
	 * @param zk
	 * @param node
	 * @param watcher
	 * @param stat
	 * @return
	 * @throws InterruptedException
	 * @throws KeeperException
	 * @return byte[]
	 * @version 1.0 2016年12月27日 下午10:30:11 created by caohui(1343965426@qq.com)
	 */
	public static byte[] safeGetData(ZooKeeper zk, String node, Watcher watcher, Stat stat) throws InterruptedException, KeeperException {
		try {
			return zk.getData(node, watcher, stat);
		}catch(KeeperException e) {
			if(e.code() != KeeperException.Code.NONODE) {
				throw e;
			}
			return new byte[]{};
		}
	}
	
	
	public static byte[] safeGetData(ZooKeeper zk, String node, boolean watch, Stat stat) throws InterruptedException, KeeperException {
		try {
			return zk.getData(node, watch, stat);
		}catch(KeeperException e) {
			if(e.code() != KeeperException.Code.NONODE) {
				throw e;
			}
			return new byte[]{};
		}
	}
	
	public static String recursiveSafeCreate(ZooKeeper zk, String node, byte[] data, List<ACL> privileges, CreateMode createMode) throws KeeperException, InterruptedException {
		if(node == null || node.length() < 0) {
			return node; // nothing to do
		}
		
		if("/".equals(node)) {
			return node; // can't create any futher
		}
		
		int index = node.lastIndexOf("/");
		if(index == -1) {
			return node; // nothing to do
		}
		
		String parent = node.substring(0, index);
		// make sure that the parent has bean created
		recursiveSafeCreate(zk, parent, data, privileges, createMode);
		
		// create this node now
		return safeCreate(zk, node, data, privileges, createMode);
		
	}
	private static class SequenceComparator implements Comparator<String> {
		private final char sequenceDelimiter;
		
		public SequenceComparator(char sequenceDelimiter) {
			this.sequenceDelimiter = sequenceDelimiter;
		}

		@Override
		public int compare(String child1, String child2) {
			long childOneSeqNbr = ZkInternalUtils.parseSequenceNumber(child1, sequenceDelimiter);
			long childTwoSeqNbr = ZkInternalUtils.parseSequenceNumber(child2, sequenceDelimiter);
			if(childOneSeqNbr < childTwoSeqNbr) {
				return -1;
			}
			
			if(childOneSeqNbr > childTwoSeqNbr) {
				return 1;
			}
			
			return 0;
		}
	}
}
