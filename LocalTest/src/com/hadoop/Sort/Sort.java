package com.hadoop.Sort;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Sort extends Configured implements Tool {
	//map将输入中的value转化成IntWritable类型，作为输出的key
	public static class Map extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
		private static IntWritable data = new IntWritable();
		public void map(LongWritable key, Text value, Context context) throws IOException,
		InterruptedException{
			System.out.println("Mapper Key " + key.toString() + " ---  Mapper value " + value.toString());
			String line = value.toString();
			data.set(Integer.parseInt(line));
			context.write(data, new IntWritable(1));
		}
	}
	
	//reduce将输入的key复制到输出的value上，然后根据输入的
	//value-list中元素的个数决定key的输出次数
	//用全局linenum来代表key的位次
	public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
		private static IntWritable linenum = new IntWritable(1);
		public void reduce(IntWritable key, Iterable<IntWritable>values, Context context)
		throws IOException, InterruptedException{
			System.out.println();
			for(IntWritable val : values){
				System.out.println("Reducer Key " + key.toString() + " ---  Reducer value " + val.toString());
				context.write(linenum, key);
				linenum=new IntWritable(linenum.get()+1);
			}
		}
	}
	
	//自定义Partition函数，此函数根据输入数据的最大值和MapReduce框架中
	//Partition的数量获取将输入数据按照大小分块的边界，然后根据输入数值和
	//边界的关系返回对应的Partition ID
	public static class Partition extends Partitioner<IntWritable, IntWritable>{
		@Override
		public int getPartition(IntWritable key, IntWritable value, int numPartitions){
			System.out.println("Partitioner Key " + key.toString() + " ---  Partitioner value " + value.toString());
			int Maxnumber = 65223;
			int bound = Maxnumber / numPartitions + 1;
			int keynumber = key.get();
			for(int i=0; i<numPartitions; i++){
				if(keynumber < bound * i && keynumber >= bound * (i - 1))
				return i - 1;
			}
			return 0;
		}
	}
	
	
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		try {
			int res = ToolRunner.run(conf, new Sort(), args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public int run(String[] args) throws Exception {
		final Job job = new Job(new Configuration(), "Sort");
		// 设置为可以打包运行
		job.setJarByClass(Sort.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setNumReduceTasks(2);
		job.setPartitionerClass(Partition.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// 清理已存在的输出文件
		FileSystem fs = FileSystem.get(new URI(args[0]), getConf());
		
		Path outPath = new Path(args[1]);
		if (fs.exists(outPath)) {
			System.out.println("Found Output Path!");
			fs.delete(outPath, true);
			System.out.println("Deleted Output Path!");
		}
		
		boolean success = job.waitForCompletion(true);
		if(success){
			System.out.println("Job process success!");
		}
		else{
			System.out.println("Job process failed!");
		}
		return 0;
	}
}
