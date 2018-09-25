package com.hadoop.maxtemperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Bean implements WritableComparable<Bean> {

    private IntWritable year; //年
    private IntWritable temperature; //温度

    public Bean() {
        set(new IntWritable(0),new IntWritable(0));
    }

    public Bean(IntWritable year, IntWritable temperature) {
        this.year = year;
        this.temperature = temperature;
    }

    public IntWritable getYear() {
        return year;
    }


    public void setYear(IntWritable year) {
        this.year = year;
    }


    public IntWritable getTemperature() {
        return temperature;
    }

    public void setTemperature(IntWritable temperature) {
        this.temperature = temperature;
    }

    public void set(IntWritable year, IntWritable temperature) {
        this.year = year;
        this.temperature = temperature;
    }

    @Override
    public int compareTo(Bean o) {

        int result = this.year.compareTo(o.year);
        if(result != 0)
            return result;
        return this.temperature.compareTo(o.temperature);

//        int result = this.temperature.compareTo(o.temperature);
//        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.year.write(dataOutput);
        this.temperature.write(dataOutput);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.year.readFields(dataInput);
        this.temperature.readFields(dataInput);
    }

    public String toString(){
        return this.getTemperature().toString();
    }

}
