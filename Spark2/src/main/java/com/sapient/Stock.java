package com.sapient;

import java.io.Serializable;

public class Stock implements Serializable{
	
	private String name;
	
	private Double stock;
	
	public Stock() {
		// TODO Auto-generated constructor stub
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Double getStock() {
		return stock;
	}

	public void setStock(Double stock) {
		this.stock = stock;
	}

	@Override
	public String toString() {
		return "Stock [name=" + name + ", stock=" + stock + "]";
	}
	
	

}
