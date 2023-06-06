package com.example.demo.exception;

public class HotelDataUploadException extends Exception {

	private String message;

	public HotelDataUploadException() {
	}

	public HotelDataUploadException(String msg) {
		super(msg);
		this.message = msg;
	}

}
