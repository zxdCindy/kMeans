import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * Calculte TF_IDF for each term in each movie
 * @author cindyzhang
 *
 */
public class TFIDFReviews extends Configured implements Tool{
	
	public static class TFIDFReviewMapper 
		extends Mapper<LongWritable, Text, Text, Text>{
		String productId = null; 
		String term = null;
		String tfValue = null;
		Text outputKey = new Text();
		Text outputValue = new Text();
		
		/**
		 * Input: DocumentId_Term, tfValue
		 * OutputKey: Term OutputValue: DocumentId_tfValue
		 */
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String[] wholeLine = value.toString().split("\t");
			productId = wholeLine[0];
			term = wholeLine[1];
			tfValue = wholeLine[2];
			
			outputKey.set(term);
			outputValue.set(productId+"\t"+tfValue);
			context.write(outputKey, outputValue);
		}
	}
	
	/**
	 * Get TF_IDF value
	 * Input: term, productId_tf value
	 * Output: productId_term, tf_idf value
	 */
	public static class TFIDFReviewsReducer 
		extends Reducer<Text, Text, Text, DoubleWritable>{
		int count = 0; //Get the number of product which contains this term
		List<String[]> valueList = new ArrayList<String[]>();
		Text outputKey = new Text();
		DoubleWritable outputValue = new DoubleWritable();
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException{
			count = 0; valueList.clear();
			for(Text value: values){
				count++;
				valueList.add(value.toString().split("\t"));
			}
			for(String[] str: valueList){
				outputKey.set(str[0]+"\t"+key.toString());
				String productNum = context.getConfiguration().get("ProductNum");
				// |D|/(1+{d: d contains t})
				double tmp = 
					Integer.valueOf(productNum) / (1+ Integer.valueOf(count) * 1.0);
				tmp = Math.log(tmp);
				// tf_idf = tf * idf;
				tmp = Double.valueOf(str[1]) * tmp;
				outputValue.set(tmp);
				context.write(outputKey, outputValue);
			}
		}
	}
	
	public int run(String[] args) throws Exception {
		String inputPath = "/Users/cindyzhang/Desktop/tf.txt";
		String outputPath = "/Users/cindyzhang/Desktop/output";
		String cachePath = "/Users/cindyzhang/NPU/CS570/HW/Project/stopWords.txt";
//		String inputPath= args[0];
//		String outputPath = args[1];
//		String cachePath = args[2];
		
        Job job = new Job(getConf(), getClass().getSimpleName()); 
        job.setJarByClass(TFReviews.class);

        // configure output and input source
        TextInputFormat.addInputPath(job, new Path(inputPath));
        job.setInputFormatClass(NLineInputFormat.class);
        job.getConfiguration().set("ProductNum", "10");
        job.getConfiguration().set(NLineInputFormat.LINES_PER_MAP,"90000");
        
        // configure mapper and reducer
        job.setMapperClass(TFIDFReviewMapper.class);
        job.setReducerClass(TFIDFReviewsReducer.class);

        // configure output
        TextOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
         
		return job.waitForCompletion(true) ? 0 : 1;
     
}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new TFIDFReviews(), args);
        System.exit(exitCode);

	}

}
