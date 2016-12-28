package com.dw.zk;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

public class Test {

	public static void main(String[] args) throws KeeperException, InterruptedException {
	   ZooKeeper zk = ZkUtils.ZK_SESSION_MANAGER.getZooKeeper();
	   System.out.println(zk.getChildren("/zookeeper", false));
	   zk.close();
	}
}
