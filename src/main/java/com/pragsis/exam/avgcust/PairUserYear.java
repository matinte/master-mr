package com.pragsis.exam.avgcust;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

//@SuppressWarnings("rawtypes")
public class PairUserYear implements WritableComparable<PairUserYear> {

    @Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + userId;
		result = prime * result + year;
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PairUserYear other = (PairUserYear) obj;
		if (userId != other.userId)
			return false;
		if (year != other.year)
			return false;
		return true;
	}
	private int year;
	private int userId;
	
	public int getYear() {
		return year;
	}
	public void setYear(int year) {
		this.year = year;
	}
	public int getUserId() {
		return userId;
	}
	public void setUserId(int userId) {
		this.userId = userId;
	}
	@Override
	public String toString() {
		return "PairUserYear [year=" + year + ", userId=" + userId
				+ ", getYear()=" + getYear() + ", getUserId()=" + getUserId()
				+ ", getClass()=" + getClass() + ", hashCode()=" + hashCode()
				+ ", toString()=" + super.toString() + "]";
	}
	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}
	@Override
	public int compareTo(PairUserYear o) {
		// TODO Auto-generated method stub
		return 0;
	}

	
}    