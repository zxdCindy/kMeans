import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Get the term dictionary and the total number of products 
 * @author cindyzhang
 *
 */
public class TermDictionary {
	
	public static class TermDictionaryMapper 
		extends Mapper<LongWritable, Text, Text, NullWritable>{
		Text outputKey = new Text();
		NullWritable outputValue = NullWritable.get();
		String productId = null; 
		String term = null;
		HashSet<String> productSet = new HashSet<String>();
		
		/**
		 * @throws InterruptedException 
		 * @throws IOException 
		 * 
		 */
		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException{
			String[] wholeLine = value.toString().split("\t");
			productId = wholeLine[0];
			term = wholeLine[1];
			productSet.add(productId);
			outputKey.set(term);				
			context.write(outputKey, outputValue);
		}
		
		/**
		 * Reduce the shuffle process' burden
		 */
		@Override
		protected void cleanup(Context context) 
				throws IOException, InterruptedException{
			for(String str: productSet){
				outputKey.set("PRODUCTID"+str);
				context.write(outputKey, outputValue);
			}
		}
	}
	
	/**
	 * The productid key-value pairs will go to the first reducer,
	 * use a global counter to get the number of products in that reducer
	 *
	 */
	public static class TermDictionaryPartitioner extends Partitioner<Text, NullWritable>{

		@Override
		public int getPartition(Text key, NullWritable value, int numReducer) {	
			if(key.toString().contains("PRODUCTID"))
				return 0;
			else
				return 1;
		}
	}
	
	public static class TermDictionaryReducer 
		extends Reducer<Text, NullWritable, Text, NullWritable>{
		public static enum MyCounters {
			ProductCounter
		};
		
		public void reduce(Text key, Iterable<NullWritable> values, Context context) 
				throws IOException, InterruptedException{
			//Count the number of products
			if(key.toString().contains("PRODUCTID")){
				context.getCounter(MyCounters.ProductCounter).increment(1);
			}
			else{
				context.write(key, NullWritable.get());
			}		
		}
	}

	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) 
			throws IOException, ClassNotFoundException, InterruptedException {
		String inputPath = args[0];//"/Users/cindyzhang/Desktop/tf.txt";//
		String outputPath = args[1];//"/Users/cindyzhang/Desktop/output";//
		
		Configuration conf = new Configuration();
		Job job = new Job(conf,"TermDictionary");
		job.setJarByClass(TermDictionary.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setMapperClass(TermDictionaryMapper.class);
		job.setPartitionerClass(TermDictionaryPartitioner.class);
		job.setReducerClass(TermDictionaryReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(2);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		//Get the counter's value
		boolean success = job.waitForCompletion(true);
		if(success){
			long pNum = 
					job.getCounters().findCounter
					(TermDictionaryReducer.MyCounters.ProductCounter).getValue();
			System.out.println("Number of Products: "+pNum);
		}
	}

}
