package com.pragsis.exam.avgcust;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class CustomGroupingComparator extends WritableComparator {
	
	public CustomGroupingComparator() {
		super(WritableComparable.class);
	}

	/** Overrides the default comparison of WritableComparables to compare 
	 * CustomKey that contain a movement in the first/left field
	 * and a year in the second/right field.  
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable v1, WritableComparable v2){

		CustomKey pair1 = (CustomKey)v1;
		CustomKey pair2 = (CustomKey)v2;
		
		//  compare the left string (name) of both pairs.  		
		int namecompare = pair1.getuserid().compareTo(pair2.getuserid());
		
		if (namecompare == 0) {
			// If names are the same, compare the right strings(years), ranking earlier years
			// as "greater than" later years (the inverse of a typical string
			// comparison)
			return -1 * pair1.getyear().compareTo(pair2.getyear());
		}
		else {
			// If names are different, we're done, return the comparison
			return namecompare;
		}
	}		
	
	
	/** 
	 * Overrides the default compare method, which is optimized for objects which
	 * can be compared byte by byte.  For Name/Year this isn't the case, so we need
	 * to read the incoming bytes to deserialize the StringPairWritable objects being
	 * compared.
	 */
	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		DataInput stream1 = new DataInputStream(new ByteArrayInputStream(b1,
				s1, l1));
		DataInput stream2 = new DataInputStream(new ByteArrayInputStream(b2,
				s2, l2));

		CustomKey v1 = new CustomKey();
		CustomKey v2 = new CustomKey();

		try {
			v1.readFields(stream1);
			v2.readFields(stream2);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return compare(v1, v2);
	}
    
	
}	
	
//	public CustomGroupingComparator() {
//
//        super(PairUserYear.class);
//    }
//
//    @Override
//    public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3, int arg4, int arg5) {
//
//        DataInput di = new DataInputStream(new ByteArrayInputStream(arg0, arg1, arg2));
//
//        DataInput di2 = new DataInputStream(new ByteArrayInputStream(arg3, arg4, arg5));
//
//        PairUserYear k1 = new PairUserYear();
//        PairUserYear k2 = new PairUserYear();
//
//        try {
//            k1.readFields(di);
//            k2.readFields(di2);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        return compare(k1, k2);
//    }
//
//    @SuppressWarnings("rawtypes")
//    @Override
//    public int compare(WritableComparable a, WritableComparable b) {
//    	PairUserYear k1 = (PairUserYear) a;
//    	PairUserYear k2 = (PairUserYear) b;
//        int c = (int) (k1.getYear() - k2.getYear());
//
//        return c;
//
//    }
//
//}
