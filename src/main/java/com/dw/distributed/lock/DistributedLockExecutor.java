package com.dw.distributed.lock;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import com.dw.exception.LockGetTimeoutException;
import com.dw.zk.ZkUtils;

/**
 * Description: 分布式锁任务执行器
 * @author caohui
 */
public final class DistributedLockExecutor {
	private DistributedLockExecutor() {}
	
	/**
	 * Description: 执行分布式加锁任务
	 * All Rights Reserved.
	 *
	 * @param job
	 * @return
	 * @throws Exception
	 * @return T
	 * @version 1.0 2016年12月27日 下午10:46:45 created by caohui(1343965426@qq.com)
	 */
	public static<T> T executeWithLock(DistributedLockJob<T> job) throws Exception {
		Lock lock = null;
		if(job.isTemp()) {
			lock = ZkUtils.getDynamicPathDistributedLock(job.getLockName());
		}else {
			lock = ZkUtils.getDistributedLock(job.getLockName());
		}
		 
		boolean lockSuccess = false;
		try {
			lockSuccess = lock.tryLock(job.getSecondCount(), TimeUnit.SECONDS);
			if(lockSuccess) {
				return job.execute();
			}
			
			throw new LockGetTimeoutException("加锁任务取锁超时："+ job.getClass().getName());
		}finally {
			if(lockSuccess) {
				lock.unlock();
			}
		}
	}

}
