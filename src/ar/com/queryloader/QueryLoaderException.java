package ar.com.queryloader;

public class QueryLoaderException extends Exception{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 9142746563989505867L;
	private String message;

	public QueryLoaderException(String message) {
		super(message);
		this.message = message;
	}

	public QueryLoaderException(String message, Throwable e) {
		super(message, e);
		this.message = message;
	}
	
	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}
	
	

}
