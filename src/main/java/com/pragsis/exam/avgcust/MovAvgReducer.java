package com.pragsis.exam.avgcust;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/* Counts the number of values associated with a key */

public class MovAvgReducer extends Reducer<CustomKey, DoubleWritable, Text, DoubleWritable> {

    @Override
    public void reduce(CustomKey key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

        /*
         * Use for loop to count items in the iterator.
         */
        double total_mov = 0;
        int total_count = 0;
        for (DoubleWritable mov : values) {
        	/*
        	 * update total_mov with mov values
        	 */
        	total_mov += mov.get();
            /*
             * for each item in the list, increment the count
             */
        	total_count++;
        }
        /*
         * calculate avg mov
         */
        double avg_mov = total_mov / total_count;
        context.write(new Text(key.getuserid() + "-" + key.getyear()), new DoubleWritable(avg_mov));
    }
}
