package com.xing.mapreduce.lesson3;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by mrz on 18-6-20.
 */
public class IpTopk {
    public static class IpTopKMapper1 extends MapReduceBase implements Mapper<LongWritable,Text,Text,Text>{

        public void map(LongWritable longWritable, Text text, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
            String ip = text.toString().split(" ",5)[0];
            outputCollector.collect(new Text(ip),new Text("1"));
        }
    }

    public static class IpTopKReduce1 extends MapReduceBase implements Reducer<Text,Text,Text,Text>{

        public void reduce(Text text, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
            long sum  = 0 ;
            while (iterator.hasNext()){
                sum = sum + Long.parseLong(iterator.next().toString());
            }
            outputCollector.collect(new Text(text),new Text(String.valueOf(sum)));
        }
    }

    public static class IpTopKMapper2 extends MapReduceBase implements Mapper<LongWritable,Text,LongWritable,Text>{

        public void map(LongWritable longWritable, Text text, OutputCollector<LongWritable, Text> outputCollector, Reporter reporter) throws IOException {
            String[] ks = text.toString().split("\t");
            outputCollector.collect(new LongWritable(Long.parseLong(ks[1])),new Text(ks[0]));
        }
    }


    public static class IPTopKReducer2 extends MapReduceBase implements Reducer<LongWritable,Text,LongWritable,Text>{

        public void reduce(LongWritable longWritable, Iterator<Text> iterator, OutputCollector<LongWritable, Text> outputCollector, Reporter reporter) throws IOException {
            while (iterator.hasNext()){
                outputCollector.collect(longWritable,iterator.next());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2){
            System.out.println("args not right");
            return;
        }

        JobConf conf = new JobConf(IpTopk.class);
//        conf.set("mapred.jar","tt.jar");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(IpTopKMapper1.class);
        conf.setCombinerClass(IpTopKReduce1.class);
        conf.setReducerClass(IpTopKReduce1.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        String inputDir = args[0];
        String outputDir = args[1];

        FileInputFormat.setInputPaths(conf,inputDir);
        FileOutputFormat.setOutputPath(conf,new Path(outputDir));

        boolean flag = JobClient.runJob(conf).isSuccessful();

        if (flag){
            System.out.println("run job - 1 successful ");
            JobConf conf1 = new JobConf(IpTopk.class);
//            conf1.set("mapred.jar","tt.jar");

            conf1.setOutputKeyClass(LongWritable.class);
            conf1.setOutputValueClass(Text.class);

            conf1.setMapperClass(IpTopKMapper2.class);
            conf1.setReducerClass(IPTopKReducer2.class);

            conf1.setInputFormat(TextInputFormat.class);
            conf1.setOutputFormat(TextOutputFormat.class);
            conf1.setNumReduceTasks(1);
            FileInputFormat.setInputPaths(conf1,outputDir);
            FileOutputFormat.setOutputPath(conf1,new Path(outputDir + "-2"));
            boolean flag1 = JobClient.runJob(conf1).isSuccessful();

            if (flag1){
                System.out.println("run job - 2 successful !!!");
            }
        }

    }






















}
