package com.xing.mapreduce.lesson2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author zhangxing
 */
public class SecondSort {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] files = new GenericOptionsParser(conf,args).getRemainingArgs();

        if (files.length < 2){
            System.out.println("usages SecondSort <in> <out>");
            System.exit(2);
        }

        Path input = new Path(files[0]);
        Path output = new Path(files[1]);

        Job  job = new Job(conf,SecondSort.class.getSimpleName());
        job.setJarByClass(SecondSort.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(newK2.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setPartitionerClass(HashPartitioner.class);
        job.setNumReduceTasks(1);
        job.setReducerClass(MyReducer.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job,output);
        FileInputFormat.setInputPaths(job,input);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
    public static class MyMapper extends Mapper<LongWritable,Text,newK2,LongWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splited = value.toString().split(" ");
            System.out.println(value.toString());
            newK2 k2 = new newK2(Long.parseLong(splited[0].trim()),Long.parseLong(splited[1].trim()));
            final LongWritable v2 = new LongWritable(Long.parseLong(splited[1]));
            context.write(k2,v2);
        }
    }

    public static class MyReducer extends Reducer<newK2,LongWritable,LongWritable,LongWritable>{
        @Override
        protected void reduce(newK2 key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            context.write(new LongWritable(key.first),new LongWritable(key.second));
        }
    }

    public static class newK2 implements WritableComparable<newK2>{

        Long first;
        Long second;

        public newK2(Long first, Long second) {
            this.first = first;
            this.second = second;
        }

        public newK2() {
        }

        @Override
        public int hashCode() {
            return this.first.hashCode() + this.second.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof newK2)){
                return false;
            }
            newK2 k2 = (newK2) obj;
            return (this.first.equals(k2.first)) && (this.second.equals(k2.second));
        }


        public int compareTo(newK2 o) {
            long temp = this.first - o.first;
            if (temp!=0){
                return (int) temp;
            }
            return (int)(this.second - o.second);
        }

        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeLong(first);
            dataOutput.writeLong(second);
        }

        public void readFields(DataInput dataInput) throws IOException {
            this.first =dataInput.readLong();
            this.second = dataInput.readLong();
        }
    }



























}
