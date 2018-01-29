package model;

import java.io.Serializable;

public class Employee implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7332556740007233777L;
	private int empNumber;
	private String managerName;
	private String month;
	private Float slaesPerMonth;
	private Float slaesPerYear;
	private Float commission;
	private int bonus;

	public Employee() {
		// TODO Auto-generated constructor stub
	}

	public int getEmpNumber() {
		return empNumber;
	}

	public void setEmpNumber(int empNumber) {
		this.empNumber = empNumber;
	}

	public String getManagerName() {
		return managerName;
	}

	public void setManagerName(String managerName) {
		this.managerName = managerName;
	}

	public String getMonth() {
		return month;
	}

	public void setMonth(String month) {
		this.month = month;
	}

	public Float getSlaesPerMonth() {
		return slaesPerMonth;
	}

	public void setSlaesPerMonth(Float slaesPerMonth) {
		this.slaesPerMonth = slaesPerMonth;
	}

	public Float getSlaesPerYear() {
		return slaesPerYear;
	}

	public void setSlaesPerYear(Float slaesPerYear) {
		this.slaesPerYear = slaesPerYear;
	}

	public Float getCommission() {
		return commission;
	}

	public void setCommission(Float commission) {
		this.commission = commission;
	}

	public int getBonus() {
		return bonus;
	}

	public void setBonus(int bonus) {
		this.bonus = bonus;
	}

	@Override
	public String toString() {
		return "Employee [empNumber=" + empNumber + ", managerName=" + managerName + ", month=" + month
				+ ", slaesPerMonth=" + slaesPerMonth + ", slaesPerYear=" + slaesPerYear + ", commission=" + commission
				+ ", bonus=" + bonus + "]";
	}

}
