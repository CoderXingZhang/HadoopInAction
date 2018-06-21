package com.xing.mapreduce.lesson2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * @author zhangxing
 */
public class querycount {
    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();
        String[] files = new GenericOptionsParser(conf,args).getRemainingArgs();
        Path input = new Path(files[0]);
        Path output = new Path(files[1]);
        Job job = new Job(conf,"query word count");
        job.setJarByClass(querycount.class);
        job.setMapperClass(MapForSogouQuery.class);
        job.setReducerClass(ReduceForSougQuery.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job,input);
        FileOutputFormat.setOutputPath(job,output);
        System.exit(job.waitForCompletion(true) ? 0 :1);
    }

    public static class MapForSogouQuery extends Mapper<LongWritable, Text, Text, IntWritable>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String lines = value.toString();
            String[] querywords = lines.split("\t");

            byte[] queryByte = querywords[2].replace("[","").replace("]","").getBytes();
            System.out.println(new String(queryByte,"UTF-8"));

            Text outputKey = new Text(new String(queryByte,"UTF-8"));
            IntWritable outPutValue = new IntWritable(1);
            context.write(outputKey, outPutValue);

        }
    }


    public static class ReduceForSougQuery extends Reducer<Text, IntWritable, Text, IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value:values) {
                sum += value.get();
            }
            context.write(key,new IntWritable(sum));
        }
    }
}

