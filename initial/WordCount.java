package initial;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

public class WordCount extends Configured implements Tool {

	public static class WordCountMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		Text outputKey = new Text();
		IntWritable ONE = new IntWritable(1);
		List<String> stopwords = new ArrayList<String>();

		@Override
		@SuppressWarnings({ "resource", "deprecation" })
		protected void setup(Context context) throws IOException {
			Path[] files = DistributedCache.getLocalCacheFiles(context
					.getConfiguration());
			if (files != null && files.length == 1) {
				System.out.println("Reading stop words from: " + files[0]);
				// Open local file to read
				DataInputStream strm = new DataInputStream(new FileInputStream(
						files[0].toString()));
				String line = strm.readLine();
				while (line != null) {
					stopwords.add(line.toString().trim());
					line = strm.readLine();
				}
			}
		}

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			if (value == null)
				return;
			String[] tokens = value.toString().split(":");
			if (tokens[0].equals("review/text")) {
				String review = tokens[1].toLowerCase().trim();
				if (review.equals("synopsis"))
					review = tokens[2].toLowerCase().trim();
				String[] terms = review.toLowerCase().split("[^a-z]+");

				for (String term : terms) {
					if (term.length()>2 && !stopwords.contains(term)) {
						outputKey.set(term);
						context.write(outputKey, ONE);
					}
				}
			}
		}
	}

	public static class WordCountReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public int run(String[] args) throws Exception {
		Job job = new Job(getConf(), getClass().getSimpleName());
		job.setJarByClass(WordCount.class);

		// configure output and input source
		TextInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(NLineInputFormat.class);
		job.getConfiguration().set(NLineInputFormat.LINES_PER_MAP, "90000");

		// configure mapper and reducer
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);

		// configure output
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// Distributed cashe
		DistributedCache.addCacheFile(
				FileSystem.get(getConf()).makeQualified(new Path(args[2]))
						.toUri(), job.getConfiguration());

		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new WordCount(), args);
		System.exit(exitCode);
	}

}
