import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Calculate TF
 * @author cindyzhang
 *
 */
public class TFReviews extends Configured implements Tool{
	
	public static class TFReviewMapper extends 
		Mapper<LongWritable, Text, Text, ReviewTuple>{
		
		Text outputKey = new Text();
		ReviewTuple outputValue = new ReviewTuple();
		List<String> stopwords = new ArrayList<String>();		
		String productId = null;
		HashMap<String,Integer> hashMap = new HashMap<String,Integer>();
		
		
		/**
		 * Filter stop words
		 */
		@Override
		@SuppressWarnings({ "resource", "deprecation" })
		protected void setup(Context context) throws IOException{
			Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			if(files!=null && files.length==1){
				System.out.println("Reading stop words from: "+
							files[0]);
				//Open local file to read
				DataInputStream strm = new DataInputStream(new FileInputStream
						(files[0].toString()));
				String line = strm.readLine();
				while(line != null){
					stopwords.add(line.toString().trim());
					line = strm.readLine();
			    }
			}
		}
		
		/**
		 * The output of the mapper is each unique term's count
		 * and the total term count in this document
		 */
		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {			
			if(value == null)
				return;
			String[] tokens = value.toString().split(":");
			if(tokens[0].equals("product/productId"))
				productId = tokens[1].trim();
			if(tokens[0].equals("review/text")){
				hashMap.clear();
				String review = tokens[1].toLowerCase().trim();
				if(review.equals("synopsis"))
					review = tokens[2].toLowerCase().trim();
				String[] terms = review.toLowerCase().split("[^a-z]+");
				
				for(String term: terms){
					if(term.length()>2 && !stopwords.contains(term)){
						if(hashMap.containsKey(term))
							hashMap.put(term, hashMap.get(term)+1);
						else
							hashMap.put(term, 1);						
					}						
				}
			}
			if(productId != null && !hashMap.isEmpty()){
				int count = 0;
				for(Entry<String, Integer> entry: hashMap.entrySet()){
					count += entry.getValue();
				}
				for(Entry<String, Integer> entry: hashMap.entrySet()){
					outputKey.set(productId + "\t" + entry.getKey());
					outputValue.setTermCount(entry.getValue());
					outputValue.setTotalCount(count);
					context.write(outputKey, outputValue);
				}							
			}
		}		
	}
	
	public static class TFReviewReducer extends 
		Reducer<Text,ReviewTuple,Text,DoubleWritable>{		
		
		DoubleWritable outputValue = new DoubleWritable();
		
	    /**
	     * tf = term count / total term count in the document
	     */
		public void reduce(Text key, Iterable<ReviewTuple> values, Context context) 
				throws IOException, InterruptedException{
			long sumTermCount = 0;
			long sumTotalCount = 0;
			for(ReviewTuple review: values){
				sumTermCount += review.getTermCount();
				sumTotalCount += review.getTotalCount();
			}
			double tf = (sumTermCount * 1.0)/ (sumTotalCount * 1.0);
			outputValue.set(tf);
			context.write(key, outputValue);			
		}
	}

	public int run(String[] args) throws Exception {
		String inputPath = "/Users/cindyzhang/NPU/CS570/HW/Project/sample.txt";
		String outputPath = "/Users/cindyzhang/Desktop/output";
		String cachePath = "/Users/cindyzhang/NPU/CS570/HW/Project/stopWords.txt";
//		String inputPath = args[0];
//		String outputPath = args[1];
//		String cachePath = args[2];
		
        Job job = new Job(getConf(), getClass().getSimpleName());               
        job.setJarByClass(TFReviews.class);

        // configure output and input source
        TextInputFormat.addInputPath(job, new Path(inputPath));
        job.setInputFormatClass(NLineInputFormat.class);
        job.getConfiguration().set(NLineInputFormat.LINES_PER_MAP,"90000");
        
        // configure mapper and reducer
        job.setMapperClass(TFReviewMapper.class);       
        job.setReducerClass(TFReviewReducer.class);

        // configure output
        TextOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ReviewTuple.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        //Distributed cashe
        DistributedCache.addCacheFile(
        			FileSystem.get(getConf()).makeQualified
				(new Path(cachePath)).toUri(), job.getConfiguration());
        
        boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
     
}
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new TFReviews(), args);
        System.exit(exitCode);
	}

}
