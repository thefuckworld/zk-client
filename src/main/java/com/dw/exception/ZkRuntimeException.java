package com.dw.exception;

public class ZkRuntimeException extends RuntimeException{
	private static final long serialVersionUID = -2153879842587896728L;

	
	public ZkRuntimeException() {}
	
	
	public ZkRuntimeException(String msg) {
		super(msg);
	}
	
	public ZkRuntimeException(Exception e) {
		super(e);
	}
	public ZkRuntimeException(String msg, Exception e) {
		super(msg, e);
	}
}
