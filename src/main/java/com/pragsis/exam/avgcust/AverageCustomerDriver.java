package com.pragsis.exam.avgcust;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class AverageCustomerDriver  {

	public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.out.printf("Usage: AverageCustomerDriver <input dir> <output dir>\n");
            System.exit(-1);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Process Logs");
        job.setJarByClass(AverageCustomerDriver.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        Path outputDir = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputDir);
        /*
         * Delete file if exists before start
         */
        FileSystem fs = outputDir.getFileSystem(conf);
        if (fs.exists(outputDir)) {
            fs.delete(outputDir, true);
        }

        job.setMapperClass(UserMovMapper.class);
        job.setReducerClass(MovAvgReducer.class);
	
        job.setMapOutputKeyClass(CustomKey.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        /*
         * Set up the partitioner. Specify 20 reducers - one for user
         */
        job.setNumReduceTasks(20);

        /*
         * Specify the partitioner class.
         */
        job.setPartitionerClass(UserPartitioner.class);

        /*
         * Specify Grouping and sorting comparators.
         */
        job.setSortComparatorClass(CustomSortingComparator.class);
        job.setGroupingComparatorClass(CustomGroupingComparator.class);
        
        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }

}
