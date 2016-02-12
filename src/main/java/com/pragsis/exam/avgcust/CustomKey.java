package com.pragsis.exam.avgcust;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CustomKey implements WritableComparable<CustomKey>{
	
	private String year;
	private String userid;
	
	public CustomKey(){};


	public CustomKey(String year, String userid) {
		super();
		this.year = year;
		this.userid = userid;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(year);
		out.writeUTF(userid);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.year = in.readUTF();
		this.userid = in.readUTF();
	}

	@Override
	public int compareTo(CustomKey o) {
		int z = year.compareTo(o.year);
		if (z == 0){
			int a = userid.compareTo(o.userid);
			if(a == 0){
				return year.compareTo(year);
			}else{
				return a;
			}
		}else{
			return z;
		}
	}


	public String getyear() {
		return year;
	}

	public void setyear(String grupo) {
		this.year = grupo;
	}

	public String getuserid() {
		return userid;
	}

	public void setuserid(String userid) {
		this.userid = userid;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((year == null) ? 0 : year.hashCode());
		result = prime * result + ((userid == null) ? 0 : userid.hashCode());
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
		CustomKey other = (CustomKey) obj;
		if (year == null) {
			if (other.year != null)
				return false;
		} else if (!year.equals(other.year))
			return false;
		if (userid == null) {
			if (other.userid != null)
				return false;
		} else if (!userid.equals(other.userid))
			return false;
		return true;
	}
	
}
