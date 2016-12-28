package com.dw.distributed.lock;

/**
 * Description: 分布式锁的操作任务
 * @author caohui
 */
public abstract class DistributedLockJob<T> {

	// 锁名称
	private String lockName;
	// 尝试取锁最大秒数
	private int secondCount;
	// 是否为用完即丢锁
	private boolean isTemp;
	
	public DistributedLockJob(String lockName, int secondCount, boolean isTemp) {
		this.lockName = lockName;
		this.secondCount = secondCount;
		this.isTemp = isTemp;
	}
	
	
	/**
	 * Description: 执行任务。该方法会被DistributedLockExecutor加分布式锁后调用。
	 * 任务异常应该抛出，并且还应该被DistributedLockExecutor抛出，交给业务代码处理。
	 * All Rights Reserved.
	 *
	 * @return
	 * @throws Exception
	 * @return T
	 * @version 1.0 2016年12月27日 下午1:07:04 created by caohui(1343965426@qq.com)
	 */
	public abstract T execute() throws Exception;


	public String getLockName() {
		return lockName;
	}


	public int getSecondCount() {
		return secondCount;
	}


	public boolean isTemp() {
		return isTemp;
	}
}
