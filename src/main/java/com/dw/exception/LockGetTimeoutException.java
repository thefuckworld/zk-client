package com.dw.exception;

public class LockGetTimeoutException extends Exception{
	private static final long serialVersionUID = -6384572210589178235L;

	public LockGetTimeoutException(String msg) {
		super(msg);
	}
}
