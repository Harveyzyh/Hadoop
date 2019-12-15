package com.hadoop.STjoin;

import java.io.IOException;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class STjoin extends Configured implements Tool{
	
	private static int time=0;
	
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		try {
			int res = ToolRunner.run(conf, new STjoin(), args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public int run(String[] args) throws Exception {
		final Job job = new Job(new Configuration(), "STjoin");
		// 设置为可以打包运行
		job.setJarByClass(STjoin.class);
		job.setMapperClass(STjoinMapper.class);
		job.setReducerClass(STjoinReducer.class);
		job.setNumReduceTasks(2);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
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
	
	public static class STjoinMapper extends Mapper<Object, Text, Text, Text>{
		protected void map(Object key, Text value, Context context) throws
				IOException, InterruptedException{
			String childname = "";
			String parentname = "";
			String relationtype = "";
			String line=value.toString();
			int i=0;
			while(line.charAt(i) != ' '){
				i++;
			}
			String[] values={line.substring(0, i), line.substring(i+1)};
			if(values[0].compareTo("child")!=0)
			{
				childname=values[0];
				parentname=values[1];
				relationtype="1";//左右表区分标志
				context.write(new Text(values[1]), new Text(relationtype
					+"+"+childname+"+"+parentname));
				//左表
				relationtype="2";
				context.write(new Text(values[0]), new Text(relationtype
					+"+"+childname+"+"+parentname));
				//右表
			}
		}
	}
	
	public static class STjoinReducer extends Reducer<Text, Text, Text, Text>{
		protected void reduce(Text key, Iterable<Text> values, Context context)throws
				IOException, InterruptedException{
			if(time==0){//输出表头
				context.write(new Text("grandchild"), new Text("grandparent"));
				time++;
			}
			int grandchildnum=0;
			String[] grandchild=new String[10];
			int grandparentnum=0;
			String[] grandparent=new String[10];
			Iterator ite=values.iterator();
			while(ite.hasNext())
			{
				String record=ite.next().toString();
				int len=record.length();
				int i=2;
				if(len==0)continue;
				char relationtype=record.charAt(0);
				String childname = "";
				String parentname = "";
//              获取value-list中value的child
				while(record.charAt(i)!='+')
				{
					childname=childname+record.charAt(i);
					i++;
				}
				i=i+1;
//              获取value-list中value的parent
				while(i<len)
				{
					parentname=parentname+record.charAt(i);
					i++;
				}
//              左表, 取出child放入grandchild
				if(relationtype=='1'){
					grandchild[grandchildnum]=childname;
					grandchildnum++;
				} else{//右表, 取出parent放入grandparent
					grandparent[grandparentnum]=parentname;
					grandparentnum++;
				}
			}
			//grandchild和grandparent数组求笛卡儿积
			if(grandparentnum!=0 && grandchildnum!=0){
				for(int m=0;m<grandchildnum;m++){
					for(int n=0;n<grandparentnum;n++){
						context.write(new Text(grandchild[m]), new Text(grandparent[n]));
//输出结果
					}
				}
			}
		}
	}
}
