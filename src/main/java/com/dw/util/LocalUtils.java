package com.dw.util;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Description: 本地资源工具类
 * @author caohui
 */
public final class LocalUtils {

	private static final Logger LOGGER = LoggerFactory.getLogger(LocalUtils.class);
	
	private LocalUtils() {}
	
	private static String LOCAL_IP = null;
	
	/**
	 * Description: 获取本机IP地址 此方法为重量级的方法，不要频繁调用
	 * All Rights Reserved.
	 *
	 * @return
	 * @return String
	 * @version 1.0 2016年11月10日 上午1:17:17 created by caohui(1343965426@qq.com)
	 */
	public static String getLocalIp() {
		if(LOCAL_IP != null) {
			return LOCAL_IP;
		}
		
		// 根据网卡取本机配置的IP
		try {
			Enumeration<NetworkInterface> netInterfaces = NetworkInterface.getNetworkInterfaces();
			String ip = null;
			a: while(netInterfaces.hasMoreElements()) {
				NetworkInterface netInterface = netInterfaces.nextElement();
				Enumeration<InetAddress> ips = netInterface.getInetAddresses();
				while(ips.hasMoreElements()) {
					InetAddress ipObject = ips.nextElement();
					if(ipObject.isSiteLocalAddress()) {
						ip = ipObject.getHostAddress();
						break a;
					}
				}
			}
			LOCAL_IP = ip;
			return ip;
		} catch (SocketException e) {
			LOGGER.error("", e);
		}
		return null;
	}
}
