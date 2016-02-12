package com.pragsis.exam.avgcust;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CustomSortingComparator extends WritableComparator {

    public CustomSortingComparator() {
        super(CustomKey.class);
    }

    @Override
    public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3, int arg4, int arg5) {

        DataInput di = new DataInputStream(new ByteArrayInputStream(arg0, arg1, arg2));

        DataInput di2 = new DataInputStream(new ByteArrayInputStream(arg3, arg4, arg5));

        CustomKey k1 = new CustomKey();
        CustomKey k2 = new CustomKey();

        try {
            k1.readFields(di);
            k2.readFields(di2);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return compare(k1, k2);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
    	CustomKey k1 = (CustomKey) a;
    	CustomKey k2 = (CustomKey) b;
        int c = (int) (Integer.parseInt(k1.getyear()) - Integer.parseInt(k2.getyear()));
        return c;
    }
}
