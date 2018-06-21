package com.xing.mapreduce.lesson1;

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
 * @author xing
 */
public class wordscount {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("yarn.resourcemanager.address", "127.0.0.1:8088");
        configuration.set("yarn.resourcemanager.scheduler.address", "127.0.0.1:8088");
        configuration.set("yarn.resourcemanager.resource-tracker.address", "127.0.0.1:8088");
        configuration.set("yarn.resourcemanager.admin.address", "127.0.0.1:8088");

        String[] files = new GenericOptionsParser(configuration, args).getRemainingArgs();
        Path input = new Path(files[0]);
        Path output = new Path(files[1]);
        Job job = new Job(configuration, "wordcount");
        job.setJarByClass(wordscount.class);
        job.setMapperClass(MapForWordcount.class);
        job.setReducerClass(ReduceForWordcount.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForWordcount extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split(" ");
            for (String word : words) {
                Text outputKey = new Text(word.trim().toUpperCase().trim());
                if ("".equals(outputKey.toString())){
                    continue;
                }
                IntWritable outputValue = new IntWritable(1);
                context.write(outputKey, outputValue);
            }
        }
    }

    public static class ReduceForWordcount extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }


}
