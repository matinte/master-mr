package com.pragsis.exam.avgcust;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserMovMapper extends Mapper<LongWritable, Text, CustomKey, DoubleWritable> {

    public static List<String> users = Arrays.asList("0","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19");

    /**
     * Example input line: 07/06/1964,7,901.94
     *
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,
            InterruptedException {

        /*
         * Split the input line into fields.
         */
        String[] fields = value.toString().split(",");

        if (fields.length > 2) {

        	/*
        	 * Splitting date fields
        	 */
        	String date = fields[0];
            String[] dtFields = date.split("/");
            if (dtFields.length > 1) {
            	String year = dtFields[2];
            	String userId = fields[1];
            	double movement = Double.parseDouble(fields[2]);
                /* 
                 * check if it's a valid user
                 */
                if (users.contains(userId)){
                    /*
                     * Save the userId and year on PairUY
                     */
                	CustomKey pair = new CustomKey();
                	pair.setuserid(userId);
                	pair.setyear(year);
                    context.write(pair, new DoubleWritable(movement)); //new Text(year + "-" + movement));
                }    
            }
        }
    }
}
