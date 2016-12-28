package com.dw.zk;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
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

import com.dw.exception.ZkRuntimeException;
import com.dw.util.PropertiesReader;

public final class ZkUtils {

	private static final Logger LOGGER = LoggerFactory.getLogger(ZkUtils.class);
	
	public static final String PROJECT_PREFIX = initPrefix();
	
	// 分布式锁默认路径
	public static final String BASE_DIR_LOCK = ZkUtils.PROJECT_PREFIX + "/lock/";
	
	private static final int DEFAULT_COUNT = 5;
	private static final int DEFAULT_MAX = 50;
	private static final int THREAD_SLEEP = 10;
	
	// 缓存同一个path下的分布式锁
	private static final ConcurrentHashMap<String, Lock> LOCK_MAP = new ConcurrentHashMap<String, Lock> ();
	private static Map<String, ZkSessionManager> zkSessionManagerMap = new ConcurrentHashMap<String, ZkSessionManager> ();
	
	private static Object obj = new Object();
	
	private ZkUtils() {}
	
	// 初始化zksessionmanager
	public static final ZkSessionManager ZK_SESSION_MANAGER = init();
	
	public static boolean isACL() throws KeeperException, InterruptedException {
		ZooKeeper zk = ZK_SESSION_MANAGER.getZooKeeper();
		byte[] data = new byte[1];
		data[0] = 0;
		zk.setData(ZkUtils.PROJECT_PREFIX + "/cluster", data, -1);
		return true;
	}
	
	/**
	 * Description: create 带有监听器的ZkSessionManager
	 * All Rights Reserved.
	 *
	 * @param listeners
	 * @return
	 * @return ZkSessionManager
	 * @version 1.0 2016年12月27日 下午3:16:15 created by caohui(1343965426@qq.com)
	 */
	public static ZkSessionManager createListenerZkSessionManager(ConnectionListener ... listeners) {
		ZkSessionManager manager = init();
		for(int i=0; i<listeners.length; i++) {
			manager.addConnectionListener(listeners[i]);
		}
		return manager;
	}
	
	public static Lock getDistributedLock(String serviceName) {
		if(ZK_SESSION_MANAGER == null) {
			throw new ZkRuntimeException("没有加载zk配置，请检查配置文件");
		}
		String path = BASE_DIR_LOCK + serviceName;
		try {
			Lock lock = LOCK_MAP.get(path);
			if(lock == null) {
				lock = new ReentrantZkLock(path, ZK_SESSION_MANAGER);
				LOCK_MAP.put(path, lock);
			}
			return lock;
		}catch(Exception e) {
			LOGGER.error("获取分布式锁异常！", e);
			throw new ZkRuntimeException("获取分布式锁异常!", e);
		}
	}
	
	private static String initPrefix() {
		Properties ps = PropertiesReader.getProperties("zkConfig");
		if(ps == null) {
			return "/dw";
		}
		
		String prefix = ps.getProperty("prefix", "/dw");
		return prefix;
	}
	
	private static ZkSessionManager init() {
		Properties ps = PropertiesReader.getProperties("zkConfig");
		String servers = ps.getProperty("servers");
		String timeout = ps.getProperty("timeout");
		if(StringUtils.isEmpty(servers) || StringUtils.isEmpty(timeout)) {
			throw new RuntimeException("zkConfig.properties 配置错误!");
		}
		ZkSessionManager manager = new DefaultZkSessionManager(servers.trim(), Integer.parseInt(timeout.trim()));
		return manager;
	}
	
	public static Lock getDynamicPathDistributedLock(String serviceName) {
		if(ZK_SESSION_MANAGER == null) {
			throw new ZkRuntimeException("没有加载zk配置，请检查配置文件");
		}
		
		String path = BASE_DIR_LOCK + serviceName;
		try {
			Lock lock = new DynamicReentrantZkLock(path, ZK_SESSION_MANAGER);
			return lock;
		}catch(Exception e) {
			LOGGER.error("获取动态分布式锁异常!", e);
			throw new ZkRuntimeException("获取动态分布式锁异常!", e);
		}
	}
	
	public static String ensureCreate(ZkSessionManager manager, String path, byte[] data, List<ACL> acl, CreateMode createMode) {
		return ensureCreate(manager, path, data, acl, createMode, 1);
	}
	
	private static String ensureCreate(ZkSessionManager manager, String path, byte[] data, List<ACL> acl, CreateMode createMode, int count) {
		String returnPath = "";
		try {
			returnPath = manager.getZooKeeper().create(path, data, acl, createMode);
		}catch(KeeperException e) {
			if(count ==  DEFAULT_MAX) {
				LOGGER.error("" , e);
				throw new ZkRuntimeException(e);
			}
			
			// 如果5次都创建失败，此线程sleep 10 ms
			if(count % DEFAULT_COUNT == 0) {
				try {
					Thread.sleep(THREAD_SLEEP);
				}catch(InterruptedException e1) {
					LOGGER.error("", e1);
				}
			}
			
			String newPath = path.trim().substring(0, path.lastIndexOf("/") - 1);
			notExitCreate(manager, newPath);
			int newCount = count + 1;
			returnPath = ensureCreate(manager, path, data, acl, createMode, newCount);
		}catch(Exception e) {
			LOGGER.error("", e);
			throw new ZkRuntimeException(e);
		}
		
		return returnPath;
	}
	
	
	
	public static boolean ensureDelete(ZkSessionManager manager, String nodeToDelete, int version) throws InterruptedException {
		try {
			manager.getZooKeeper().delete(nodeToDelete, version);
			return true;
		}catch(KeeperException e) {
			// if the node has already bean deleted, don't worry about it.
			if(e.code() != KeeperException.Code.NONODE) {
				LOGGER.error(e.getMessage(), e);
				Thread.sleep(THREAD_SLEEP);
				return ensureDelete(manager, nodeToDelete, version);
			}
			return false;
		}catch(Throwable t) {
			LOGGER.error(t.getMessage(), t);
			Thread.sleep(THREAD_SLEEP);
			return ensureDelete(manager, nodeToDelete, version);
		}
	}
	/**
	 * Description: 如果不存在 zk path, create path
	 * All Rights Reserved.
	 *
	 * @param manager
	 * @param path
	 * @return
	 * @return boolean
	 * @version 1.0 2016年12月27日 下午1:53:22 created by caohui(1343965426@qq.com)
	 */
	public static boolean notExitCreate(ZkSessionManager manager, String path) {
		boolean result = false;
		try {
			Stat stat = manager.getZooKeeper().exists(path, false);
			if(stat == null) {
				String[] paths = path.split("/");
				String part = "";
				for(int i=1; i<paths.length; i++) {
					part = part + "/" + paths[i];
					if(manager.getZooKeeper().exists(part, false) == null) {
						manager.getZooKeeper().create(part, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					}
				}
			}
			result = true;
		}catch(KeeperException.NodeExistsException e) {
			// 节点存在异常忽略
			LOGGER.info("创建存在的节点异常：{}", e.getMessage());
		}catch(Exception e) {
			result = false;
			LOGGER.error("create zk path exception!", e);
			throw new ZkRuntimeException("create zk path exception !", e);
		}
		return result;
	}
	
	public static ZkSessionManager getInstance(String servers, String timeout) {
		ZkSessionManager manager = zkSessionManagerMap.get(servers);
		if(manager == null) {
			synchronized(obj) {
				manager = zkSessionManagerMap.get(servers);
				if(manager != null) {
					return manager;
				}
				
				manager = new DefaultZkSessionManager(servers.trim(), Integer.parseInt(timeout.trim()));
				if(manager != null) {
					zkSessionManagerMap.put(servers, manager);
					return manager;
				}
			}
		}
		return manager;
	}
}
