package com.xing.mapreduce.lesson3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Comparator;
import java.util.TreeMap;

/**
 * @author xing
 */
public class TopK {
    private static final int K = 10;
    public static class KMap extends Mapper<LongWritable,Text,IntWritable,Text>{

        TreeMap<Integer,String> treeMap = new TreeMap<Integer, String>(new Comparator<Integer>() {
            public int compare(Integer o1, Integer o2) {
                return o2.compareTo(o1);
            }
        });
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line =value.toString();
            if (line.trim().length() > 0){
                String[] arr= line.split(" ",2);
                String name = arr[0];
                Integer num = Integer.parseInt(arr[1]);
                treeMap.put(num,name);
                if (treeMap.size() > K){
                    treeMap.remove(treeMap.firstKey());
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Integer num : treeMap.keySet()) {
                context.write(new IntWritable(num),new Text(treeMap.get(num)));
            }
        }
    }

    public static class KReduce extends Reducer<IntWritable,Text,Text,IntWritable>{
        TreeMap<Integer,String> map = new TreeMap<Integer, String>(new Comparator<Integer>() {
            public int compare(Integer o1, Integer o2) {
                return o2.compareTo(o1);
            }
        });

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            map.put(key.get(),values.iterator().next().toString());
            if (map.size() > K){
                map.remove(map.firstKey());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Integer num: map.keySet()) {
                context.write(new Text(map.get(num)),new IntWritable(num));
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] files = new GenericOptionsParser(conf,args).getRemainingArgs();
        if (files.length < 2){
            System.out.println("usages TopK  <in> <out>");
            System.exit(2);
        }
        Path input = new Path(files[0]);
        Path output = new Path(files[1]);

        Job job = new Job(conf,TopK.class.getSimpleName());
        job.setJarByClass(TopK.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(KMap.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
//        job.setPartitionerClass(HashPartitioner.class);
        job.setReducerClass(KReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job,output);
        FileInputFormat.setInputPaths(job,input);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }





}






























