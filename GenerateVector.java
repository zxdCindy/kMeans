import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class GenerateVector {
	
	public static class GenerateVectorMapper 
		extends Mapper<LongWritable, Text, Text, Text>{
		String productId = null;
		String term = null;
		String tfScore = null;
		Text outputKey = new Text();
		Text outputValue = new Text();		
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String[] wholeLine = value.toString().split("\t");
			productId = wholeLine[0];
			term = wholeLine[1];
			tfScore = wholeLine[2];
			outputKey.set(productId);
			outputValue.set(term+"\t"+tfScore);
			context.write(outputKey, outputValue);
		}
	}
	
	public static class GenerateVectorReducer extends Reducer<Text,Text,Text,Text>{
		List<String> dictionary = new ArrayList<String>();
		HashMap<String, String> termMap = new HashMap<String,String>();
		StringBuffer result = new StringBuffer();
		Text outputValue = new Text();
		
		/**
		 * Read the dictionary from the distributed file
		 */
		@Override
		protected void setup(Context context) throws IOException{
			Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			if(files!=null && files.length==1){
				//Open local file to read
				DataInputStream strm = new DataInputStream(new FileInputStream
						(files[0].toString()));
				String line = strm.readLine();
				while(line != null){
					dictionary.add(line.toString().trim());
					line = strm.readLine();
			    }
			}
		}
		
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			termMap.clear();
			result = new StringBuffer();
			for(Text value: values){
				String[] str = value.toString().split("\t");
				termMap.put(str[0], str[1]);				
			}
			for(String term: dictionary){
				if(termMap.containsKey(term)){
					result.append(termMap.get(term)+" ");
				}
				else{
					result.append("0 ");
				}
			}
			outputValue.set(result.toString());
			context.write(key, outputValue);
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
		String inputPath = "/Users/cindyzhang/Desktop/tfidf.txt";//args[0];//
		String outputPath = "/Users/cindyzhang/Desktop/output";//args[1];//
		String cachePath = "/Users/cindyzhang/Desktop/dictionary.txt";//args[2];
		
		Configuration conf = new Configuration();
		Job job = new Job(conf,"GenerateVector");
		job.setJarByClass(GenerateVector.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(GenerateVectorMapper.class);
		job.setReducerClass(GenerateVectorReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		 //Distributed cashe
        DistributedCache.addCacheFile(
        			FileSystem.get(conf).makeQualified
				(new Path(cachePath)).toUri(), job.getConfiguration());
        job.waitForCompletion(true);
	}

}
