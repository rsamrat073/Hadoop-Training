package com.sapient.www;

import java.sql.Date;

public class Stock {

	private String exchange;
	private String tickersymbol;
	private String date;
	private Float low;
	private Float mid;
	private Float high;
	private Float close;
	private int isin;

	public String getExchange() {
		return exchange;
	}

	public void setExchange(String exchange) {
		this.exchange = exchange;
	}

	public String getTickersymbol() {
		return tickersymbol;
	}

	public void setTickersymbol(String tickersymbol) {
		this.tickersymbol = tickersymbol;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public Float getLow() {
		return low;
	}

	public void setLow(Float low) {
		this.low = low;
	}

	public Float getMid() {
		return mid;
	}

	public void setMid(Float mid) {
		this.mid = mid;
	}

	public Float getHigh() {
		return high;
	}

	public void setHigh(Float high) {
		this.high = high;
	}

	public Float getClose() {
		return close;
	}

	public void setClose(Float close) {
		this.close = close;
	}

	public int getIsin() {
		return isin;
	}

	public void setIsin(int isin) {
		this.isin = isin;
	}

}
