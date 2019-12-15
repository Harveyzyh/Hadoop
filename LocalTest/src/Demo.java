//package XXX;

import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class Demo extends Configured implements Tool{
	
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		try {
			int res = ToolRunner.run(conf, new Demo(), args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public int run(String[] args) throws Exception {
		final Job job = new Job(new Configuration(), "WebLogHandel");
		// 设置为可以打包运行
		job.setJarByClass(Demo.class);
		FileInputFormat.setInputPaths(job, args[0]);
		job.setMapperClass(Map.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// 清理已存在的输出文件
		FileSystem fs = FileSystem.get(new URI(args[0]), getConf());
		Path outPath = new Path(args[1]);
		if (fs.exists(outPath)) {
			fs.delete(outPath, true);
		}
		
		boolean success = job.waitForCompletion(true);
		if(success){
			System.out.println("Clean process success!");
		}
		else{
			System.out.println("Clean process failed!");
		}
		return 0;
	}
	
	static class Map extends Mapper<IntWritable, IntWritable, IntWritable, IntWritable>{
		protected void map(IntWritable key, IntWritable value, Context context) throws
				java.io.IOException, InterruptedException{
			
		}
	}
	
	static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
		protected void reduce(IntWritable key, IntWritable value, Context context) throws
				java.io.IOException, InterruptedException{
			
		}
	}
}
