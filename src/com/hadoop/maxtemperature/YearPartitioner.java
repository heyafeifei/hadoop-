package com.hadoop.maxtemperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;
import java.util.Map;

public class YearPartitioner extends Partitioner<Text, IntWritable> {
    private static Map<String,Integer> years = new HashMap<>();
    static {
        //这里给每一个编制一个分区
        years.put("1901",0);
        years.put("1902",1);

    }
    @Override
    public int getPartition(Text text, IntWritable intWritable, int i) {
        Integer year = years.get(text.toString());
        year = year == null ? 9999 : year;  //如果在列表中找不到，则指定一个默认的分区
        return year;
    }


}
