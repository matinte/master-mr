package com.pragsis.exam.avgcust;

import java.util.HashMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class UserPartitioner <K2, V2> extends Partitioner<CustomKey, DoubleWritable> {

    HashMap<String, Integer> users = new HashMap<String, Integer>();

    /**
     * Set up the users hash map.
     */
    public UserPartitioner() {
    	users.put("0", 0);
    	users.put("1", 1);
    	users.put("2", 2);
    	users.put("3", 3);
        users.put("4", 4);
        users.put("5", 5);
        users.put("6", 6);
        users.put("7", 7);
        users.put("8", 8);
        users.put("9", 9);
        users.put("10", 10);
        users.put("11", 11);
        users.put("12", 12);
        users.put("13", 13);
        users.put("14", 14);
        users.put("15", 15);
        users.put("16", 16);
        users.put("17", 17);
        users.put("18", 18);
        users.put("19", 19);
    }

    /**
     * Retrieve userId from CustomKey and check whether it is or not in users List
     */
    @Override
    public int getPartition(CustomKey key, DoubleWritable value, int numReduceTasks) {
        return (int) (users.get(key.getuserid().toString()));
    }
}