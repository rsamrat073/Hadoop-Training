package model;

import java.io.Serializable;

public class Customer implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 5538620937745865127L;
	private int customerID;
	private String customerFirstName;
	private String customerSecondName;
	private String customerEmailID;
	private String customerPassword;
	private String customerStreetAddr;
	private String customerCity;
	private String customerState;
	private int customerAddrZip;
	
	
	public Customer() {
		// TODO Auto-generated constructor stub
	}


	public int getCustomerID() {
		return customerID;
	}


	public void setCustomerID(int customerID) {
		this.customerID = customerID;
	}


	public String getCustomerFirstName() {
		return customerFirstName;
	}


	public void setCustomerFirstName(String customerFirstName) {
		this.customerFirstName = customerFirstName;
	}


	public String getCustomerSecondName() {
		return customerSecondName;
	}


	public void setCustomerSecondName(String customerSecondName) {
		this.customerSecondName = customerSecondName;
	}


	public String getCustomerEmailID() {
		return customerEmailID;
	}


	public void setCustomerEmailID(String customerEmailID) {
		this.customerEmailID = customerEmailID;
	}


	public String getCustomerPassword() {
		return customerPassword;
	}


	public void setCustomerPassword(String customerPassword) {
		this.customerPassword = customerPassword;
	}


	public String getCustomerStreetAddr() {
		return customerStreetAddr;
	}


	public void setCustomerStreetAddr(String customerStreetAddr) {
		this.customerStreetAddr = customerStreetAddr;
	}


	public String getCustomerCity() {
		return customerCity;
	}


	public void setCustomerCity(String customerCity) {
		this.customerCity = customerCity;
	}


	public String getCustomerState() {
		return customerState;
	}


	public void setCustomerState(String customerState) {
		this.customerState = customerState;
	}


	public int getCustomerAddrZip() {
		return customerAddrZip;
	}


	public void setCustomerAddrZip(int customerAddrZip) {
		this.customerAddrZip = customerAddrZip;
	}


	@Override
	public String toString() {
		return "Customer [customerID=" + customerID + ", customerFirstName=" + customerFirstName
				+ ", customerSecondName=" + customerSecondName + ", customerEmailID=" + customerEmailID
				+ ", customerPassword=" + customerPassword + ", customerStreetAddr=" + customerStreetAddr
				+ ", customerCity=" + customerCity + ", customerState=" + customerState + ", customerAddrZip="
				+ customerAddrZip + "]";
	}
	
	
	
}
