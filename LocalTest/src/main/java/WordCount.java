import java.io.File;
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

public class WordCount {
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		
		private final static IntWritable one = new IntWritable(1);
//		private Text word = new Text();
		
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
//				word.set(itr.nextToken());
				Text word = new Text(itr.nextToken());
				context.write(word, one);
			}
		}
	}
	
	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int wordCount = 0;
			for (IntWritable val : values) {
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
	
	// 清除目录
	private static void deleteDirectory(File file){
		if(file.exists()){
			if(file.isFile()){
				file.delete();//清理文件
			}else{
				File list[] = file.listFiles();
				if(list!=null){
					for(File f : list){
						deleteDirectory(f);
					}
					file.delete();//清理目录
				}
			}
		}
	}
	
	// 主函数
	public static void main(String[] args) throws Exception {
		deleteDirectory(new File("./" + args[2].substring(0, args[2].length() - 1))); //先清除output目录
		
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
}
