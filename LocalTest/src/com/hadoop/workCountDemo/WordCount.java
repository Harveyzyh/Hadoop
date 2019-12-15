package com.hadoop.workCountDemo;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.hadoop.lib.DirOpt;

public class WordCount {
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		
		private final static IntWritable one = new IntWritable(1);
		Text word = new Text();
		
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			System.out.println("Mapper Key " + key.toString() + " ---  Mapper value " + value.toString());
			
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word = new Text(itr.nextToken());
				context.write(word, one);
			}
		}
	}
	
	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			System.out.println();
			int wordCount = 0;
			for (IntWritable val : values) {
				System.out.println("Reducer Key " + key.toString() + " ---  Reducer value " + val.toString());
				wordCount += val.get();
			}
			result.set(wordCount);
			context.write(key, result);
		}
	}
	
	public static class MyCombiner extends Reducer<Text, IntWritable, Text, IntWritable>{
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException{
			
		}
	}
	
	// 主函数
	public static void main(String[] args) throws Exception {
		if(args.length == 3){
			System.out.println("agr0: " + args[0]);
			System.out.println("agr1: " + args[1]);
			System.out.println("agr2: " + args[2]);
			
			//先清除output目录
			DirOpt.deleteDirectory(args[2], true);
			
			//job配置
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, args[0]);
			job.setJarByClass(WordCount.class);
			job.setMapperClass(TokenizerMapper.class);
			job.setCombinerClass(IntSumReducer.class);
			job.setReducerClass(IntSumReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(job, new Path(args[1]));
			FileOutputFormat.setOutputPath(job, new Path(args[2]));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
		else{
			System.out.println("参数不全， 传入参数个数需3个，当前传入" + args.length + "个");
		}
	}
}
