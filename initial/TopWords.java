package initial;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TopWords {
	private final static int TOPNUM = 1000;
	private static Comparator<Map.Entry<String, Integer>> MyComparator = new Comparator<Map.Entry<String, Integer>>() {

		@Override
		public int compare(Entry<String, Integer> entry1,
				Entry<String, Integer> entry2) {
			int num1 = entry1.getValue() - entry2.getValue();
			int num2 = entry1.getKey().compareTo(entry2.getKey());
			if(num1 != 0)
				return num1;
			else
				return num2;
		}
	};

	public static class TopWordsMapper extends
			Mapper<Object, Text, Text, IntWritable> {
		Text outputKey = new Text();
		IntWritable outputValue = new IntWritable();

		private TreeSet<Map.Entry<String, Integer>> treeSet = 
				new TreeSet<Map.Entry<String, Integer>>(MyComparator);

		public void map(Object key, Text value, Context context){
			String[] values = value.toString().split("\t");
			MyEntry<String, Integer> entry = 
					new MyEntry<String, Integer>(values[0], Integer.parseInt(values[1]));
			treeSet.add(entry);
			if(treeSet.size() > TOPNUM)
				treeSet.remove(treeSet.first());			
		}

		// Gets called once after all key/value pairs have been through map
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			for (Map.Entry<String, Integer> entry: treeSet) {
				outputKey.set(entry.getKey());
				outputValue.set(entry.getValue());
				context.write(outputKey,outputValue);
			}
		}
	}
	
	public static class TopWordsReducer 
	extends Reducer<Text, IntWritable, Text, IntWritable>{
	private TreeMap<Integer,Text> repToRecordMap = new TreeMap<Integer, Text>();
	
	Text outputKey = new Text();
	IntWritable outputValue = new IntWritable();

	private TreeSet<Map.Entry<String, Integer>> treeSet = 
			new TreeSet<Map.Entry<String, Integer>>(MyComparator);
	
	public void reduce(Text key, Iterable<Integer> values, Context context) 
			throws IOException, InterruptedException{
		Integer max = 0;
		for(Integer value: values ){
			if(value > max)
				max = value;
		}
		MyEntry<String, Integer> entry = 
				new MyEntry<String, Integer>(key.toString(), max);
		treeSet.add(entry);
		if(treeSet.size() > TOPNUM)
			treeSet.remove(treeSet.first());			
	}

	// Gets called once after all key/value pairs have been through map
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		for (Map.Entry<String, Integer> entry: treeSet) {
			outputKey.set(entry.getKey());
			outputValue.set(entry.getValue());
			context.write(outputKey,outputValue);
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
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(args).getRemainingArgs();
		Job job = new Job(conf, "TopWords");
		job.setJarByClass(TopWords.class);
		job.setMapperClass(TopWordsMapper.class);
		job.setCombinerClass(TopWordsReducer.class);
		job.setReducerClass(TopWordsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true)?0:1);

	}

}
