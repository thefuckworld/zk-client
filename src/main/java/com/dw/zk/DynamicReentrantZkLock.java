package com.dw.zk;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dw.zk.ZkSessionManager;

/**
 * Description: 动态分布式锁实现
 * @author caohui
 */
public class DynamicReentrantZkLock extends ReentrantZkLock{

	private static final Logger LOGGER = LoggerFactory.getLogger(DynamicReentrantZkLock.class);
	public DynamicReentrantZkLock(String baseNode, ZkSessionManager zkSessionManager) {
		super(baseNode, zkSessionManager);
	}
	@Override
	public void unlock() {
		super.unlock();
		LockHolder nodeToRemove = locks.get();
		if(nodeToRemove == null) {
			try {
				ZkInternalUtils.safeDelete(zkSessionManager.getZooKeeper(), baseNode, -1);
			}catch(KeeperException.NotEmptyException e) {
				LOGGER.info("移除动态节点不为空:{}", e.getMessage());
			}catch(Exception e) {
				LOGGER.error("", e);
			}
		}
	}
	
}
