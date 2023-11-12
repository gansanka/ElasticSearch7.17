package com.example.es.data;

import java.util.Date;

public class Product {

	Integer id;
	String name;
	Date from;
	Date to;

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Date getFrom() {
		return from;
	}

	public void setFrom(Date from) {
		this.from = from;
	}

	public Date getTo() {
		return to;
	}

	public void setTo(Date to) {
		this.to = to;
	}

	@Override
	public String toString() {
		return "id : " + this.id + ", name : " + this.name + ", from : " + this.from + ", to : " + this.to;
	}
}
