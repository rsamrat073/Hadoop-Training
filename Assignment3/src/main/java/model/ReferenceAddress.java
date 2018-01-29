package model;

import java.io.Serializable;

public class ReferenceAddress implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -5219001695515649734L;
	private String completeAddress;
	private String shortFormAddress;
	
	
	public ReferenceAddress() {
		// TODO Auto-generated constructor stub
	}


	public String getCompleteAddress() {
		return completeAddress;
	}


	public void setCompleteAddress(String completeAddress) {
		this.completeAddress = completeAddress;
	}


	public String getShortFormAddress() {
		return shortFormAddress;
	}


	public void setShortFormAddress(String shortFormAddress) {
		this.shortFormAddress = shortFormAddress;
	}


	@Override
	public String toString() {
		return "ReferenceAddress [completeAddress=" + completeAddress + ", shortFormAddress=" + shortFormAddress + "]";
	}
	
	
	
}
