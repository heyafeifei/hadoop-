package com.hadoop.maxtemperature;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTemperature {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MaxTemperature <input path> <output path>");
            System.exit(-1);
        }
        Configuration config = new Configuration();

        Job job = Job.getInstance(config);
        job.setJarByClass(MaxTemperature.class);
        job.setJobName("Max temperature");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MaxTemperatureMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);


        job.setOutputKeyClass(Text.class);              //注1
        job.setOutputValueClass(IntWritable.class);


        job.setPartitionerClass(YearPartitioner.class);//指定数据分区规则，不是必须要的，根据业务需求分区
        job.setNumReduceTasks(2); //设置相应的reducer数量，这个数量要与分区的大最数量一致


        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
