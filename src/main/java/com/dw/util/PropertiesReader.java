package com.dw.util;

import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public final class PropertiesReader {

	// 缓存properties文件
	private static Map<String, Properties> propertiesMap = new ConcurrentHashMap<String, Properties>();
	// 默认property配置文件后缀
	private static final String DEFAULT = "_defalut";
	// 缓存有序properties文件
	private static Map<String, OrderProperties> orderPropertiesMap = new ConcurrentHashMap<String, OrderProperties> ();
	
	private PropertiesReader() {}
	
	public static<T> T getAppointPropertiesAttribute(String propertiesName, String attributeName, Class<T> clazz) {
	   Assert.notNull(clazz);	
	   Assert.isExist(new Object[] {Integer.class, Boolean.class, String.class, Long.class, Double.class}, clazz, "");
	   
	   Properties ps = getProperties(propertiesName);
	   
	   return getAppointPropertiesAttribute(ps, attributeName, clazz);
	}
	
	
	public static<T> T getAppointPropertiesAttribute(Properties ps, String attributeName, Class<T> clazz) {
		Assert.notNull(clazz);
		Assert.isExist(new Object[] {Integer.class, Boolean.class, String.class, Long.class, Double.class}, clazz, "");
		
		if(ps != null) {
			String value = ps.getProperty(attributeName);
			if(value == null || "".equals(value)) {
				return null;
			}
			
			if(clazz == Boolean.class) {
				return clazz.cast(Boolean.parseBoolean(value));
			}
			
			if(clazz == Integer.class) {
				return clazz.cast(Integer.parseInt(value));
			}
			
			if(clazz == Long.class) {
				return clazz.cast(Long.parseLong(value));
			}
			
			if(clazz == Double.class) {
				return clazz.cast(Double.parseDouble(value));
			}
			
			return clazz.cast(value);
		}
		
		return null;
	}
	
	
	public static Properties getProperties(String propertiesName) {
		return getProperties(propertiesName, true);
	}
	
	public static Properties getProperties(String propertiesName, boolean isCache) {
		Properties resultProperties = propertiesMap.get(propertiesName);
		// 缓存没有，重新读取并存入缓存
		if(resultProperties == null) {
			Properties defaultProp = createProperties(propertiesName + DEFAULT); // 获取默认property文件
			resultProperties = createProperties(propertiesName);
			if(defaultProp != null && resultProperties != null) {
				resultProperties.putAll(defaultProp);
			}else if(defaultProp != null) {
				resultProperties = defaultProp;
			}
			
			if(isCache) {
				if(resultProperties != null) {
					propertiesMap.put(propertiesName, resultProperties);
				}
			}
		}
		return resultProperties;
	}
	
	public static OrderProperties getOrderProperties(String propertiesName) {
		OrderProperties resultProperties = orderPropertiesMap.get(propertiesName);
		
		// 缓存没有，重新读取并存入缓存
		if(resultProperties == null) {
			OrderProperties defaultProp = createOrderProperties(propertiesName + DEFAULT) ;
			resultProperties = createOrderProperties(propertiesName);
			if(defaultProp != null && resultProperties != null) {
				resultProperties.putAll(defaultProp);
			}else if(defaultProp != null) {
				resultProperties = defaultProp;
			}
			
			if(resultProperties != null) {
				orderPropertiesMap.put(propertiesName, resultProperties);
			}
		}
		
		return resultProperties;
	}
	
	private static Properties createProperties(String propertiesName) {
		InputStream fis = null;
		Properties properties = null;
		try {
			fis = PropertiesReader.class.getResourceAsStream("/" + propertiesName + ".properties");
			if(fis != null) {
				properties = new Properties();
				properties.load(fis);
				fis.close();
			}
		}catch(Exception e) {
			fis = null;
		}
		return properties;
	}
	
	
	private static OrderProperties createOrderProperties(String propertiesName) {
		InputStream fis = null;
		OrderProperties properties = null;
		try {
			fis = PropertiesReader.class.getResourceAsStream("/" + propertiesName + ".properties");
			if(fis != null) {
				properties = new OrderProperties();
				properties.load(fis);
				fis.close();
			}
		}catch(Exception e) {
			fis = null;
		}
		
		return properties;
	}
}
