package com.hadoop.webLogHandelDemo;

import java.io.IOException;
import java.util.StringTokenizer;

import com.hadoop.lib.DirOpt;
import com.hadoop.workCountDemo.WordCount;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WebLog {
	public static void main(String[] args) throws Exception{
		if(args.length == 3){
			System.out.println("agr0: " + args[0]);
			System.out.println("agr1: " + args[1]);
			System.out.println("agr2: " + args[2]);
			
			//先清除output目录
			DirOpt.deleteDirectory(args[2], true);
			
			//job配置
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, args[0]);
			job.setJarByClass(WebLog.class);
			job.setMapperClass(WebLog.SelfMapper.class);
			job.setCombinerClass(WebLog.SelfReducer.class);
			job.setReducerClass(WebLog.SelfReducer.class);
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
	
	public class SelfMapper extends Mapper<Object, Text, Text, IntWritable>{
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		
		}
	}
	
	public class SelfCombiner extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			
		}
	}
	
	public class SelfReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			
		}
	}
}


