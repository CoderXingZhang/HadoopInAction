package com.xing.mapreduce.lesson3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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
import java.util.Map;
import java.util.TreeMap;

/**
 *　@author zhangxing
 *  #orderid(订单号),userid(用户名),payment(付款额),productid(产品编号)
     求topN的payment 对应的用户名　
 */
public class TokNPayment {
    private static final int K = 5;

    public static class TopPMapper extends Mapper<LongWritable,Text,IntWritable,Text>{
        TreeMap<Integer,String> treeMap = new TreeMap<Integer, String>(new Comparator<Integer>() {
            public int compare(Integer o1, Integer o2) {
                return o2.compareTo(o1);
            }
        });

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(",");
            String uerId = line[1];
            Integer payment = Integer.parseInt(line[2]);
            if (treeMap.containsKey(payment)){
                treeMap.put(payment,treeMap.get(payment) + ","+uerId);
            }else {
                treeMap.put(payment,uerId);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Integer, String> entry : treeMap.entrySet()) {
                context.write(new IntWritable(entry.getKey()), new Text(entry.getValue()));
            }
        }
    }

    public static class TopPReducer extends Reducer<IntWritable,Text,Text,IntWritable>{
        TreeMap<Integer,String> treeMap = new TreeMap<Integer, String>(new Comparator<Integer>() {
            public int compare(Integer o1, Integer o2) {
                return o2.compareTo(o1);
            }
        });

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            treeMap.put(key.get(),values.iterator().next().toString());
            if (treeMap.size() > K){
                treeMap.remove(treeMap.lastKey());
            }
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Integer,String> entry:treeMap.entrySet()) {
                context.write(new Text(entry.getValue()),new IntWritable(entry.getKey()));
            }
        }
    }

    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();
        String[] files = new GenericOptionsParser(conf,args).getRemainingArgs();
        if (files.length < 2){
            System.out.println("usages TopNPayment  <in> <out>");
            System.exit(2);
        }
        Path input = new Path(files[0]);
        Path output = new Path(files[1]);

        Job job = new Job(conf,TokNPayment.class.getSimpleName());
        job.setJarByClass(TopK.class);
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(TopPMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
//        job.setPartitionerClass(HashPartitioner.class);
        job.setReducerClass(TopPReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job,output);
        FileInputFormat.setInputPaths(job,input);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
