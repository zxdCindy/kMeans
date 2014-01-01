
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.VectorWritable;


/**
 * Get the initialization centroids
 * @author cindyzhang
 *
 */
public class ChooseCentroids {

	public static class CentroidMapper extends 
		Mapper<LongWritable, Text, DoubleWritable, VectorWritable>{		
		List<Vector> centroid = new ArrayList<Vector>();
		VectorWritable vectorWritable = new VectorWritable();
		DoubleWritable doubleWritable = new DoubleWritable();
		//Get the current centroids first
		@Override
		protected void setup(Context context) throws IOException{
			DataInputStream str = new DataInputStream
					(new FileInputStream(context.getConfiguration().get("centroidPath")));
			String line = str.readLine();
			while(line != null){
				String tmpStr = line.split("\t")[1];
				centroid.add(convertToVector(tmpStr));
				line = str.readLine();
			}
		}
		
		/**
		 * Get the minDistance from a point to each of the centroids
		 * @param key
		 * @param value
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException{
			String line = value.toString();
			String tmpStr = line.split("\t")[1];
			Vector pointVector = convertToVector(tmpStr);
			double minDistance = 0.0;
			if(!centroid.contains(pointVector)){
				for(Vector vector: centroid){
					double distance = new EuclideanDistanceMeasure().distance(pointVector, vector);
					if(distance < minDistance || minDistance == 0.0){
						minDistance = distance;
					}				
				}
				vectorWritable.set(pointVector);
				doubleWritable.set(minDistance);
				context.write(doubleWritable, vectorWritable);
			}						
		}
	}
	
	/**
	 * Choose the maximum distance among all the minimum distances,
	 * set this point as the new centroid
	 *
	 */
	public static class CentroidReducer 
		extends Reducer<DoubleWritable, VectorWritable, DoubleWritable, Text>{
		VectorWritable maxVector;
		double maxDistance = 0.0;
		DoubleWritable doubleWritable = new DoubleWritable();
		StringBuffer result = new StringBuffer();
		Text outputKey = new Text();
		
		@Override
		public void reduce(DoubleWritable key, Iterable<VectorWritable> values, Context context){
			double distance = key.get();
			if(distance > maxDistance){
				maxDistance = distance;
				doubleWritable.set(maxDistance);
				// Choose any vector with the same least distance from the set of centroids
				maxVector = values.iterator().next();
			}
		}
		
		@Override
		protected void cleanup(Context context) 
				throws IOException, InterruptedException{
			result = new StringBuffer();
			for(Element score: maxVector.get().all()){
				result.append(String.valueOf(score.get())+" ");
			}
			outputKey.set(result.toString());
			context.write(doubleWritable, outputKey);
		}
	}
	
	/**
	 * Convert a string to a mahout vector
	 * @param str
	 * @return
	 */
	public static Vector convertToVector(String str){
		String[] strPoint = str.split(" ");
		double[] numPoint = new double[strPoint.length];
		for(int i=0; i<strPoint.length;i++){
			numPoint[i]=Double.parseDouble(strPoint[i].trim());
		}
		return new DenseVector(numPoint);
	}
	
	public static void main(String[] args) throws Exception {
		int N = 3; // the number of centroids
		String inputPath = "/Users/cindyzhang/Desktop/vector.txt"; //args[0];
		String outputPath = "/Users/cindyzhang/Desktop/output"; //args[1];
		String cachePath = "/Users/cindyzhang/Desktop/centroid1.txt"; 
		Configuration conf = new Configuration();
		conf.set("centroidPath", cachePath);
		Job job = new Job(conf, "ChooseCentroids");
		job.setJarByClass(ChooseCentroids.class);
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(VectorWritable.class);
		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(CentroidMapper.class);
		job.setReducerClass(CentroidReducer.class);
		job.setNumReduceTasks(1);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.waitForCompletion(true);
		
	}

}
