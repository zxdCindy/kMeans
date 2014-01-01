import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.hadoop.similarity.cooccurrence.Vectors;

/**
 * KMEANS implementation
 * @author cindyzhang
 *
 */
public class kmeans {
	
	public static final String SEPERATE = " ";

	public static class kmeansMapper 
		extends Mapper<LongWritable, Text, Text, VectorWritable>{		
		List<Vector> centroid = new ArrayList<Vector>();
		Text outputKey = new Text();
		VectorWritable outputValue = new VectorWritable();;
		
		@Override
		protected void setup(Context context) throws IOException{
			DataInputStream str = new DataInputStream
					(new FileInputStream(context.getConfiguration().get("centroidPath")));
			String line = str.readLine();
			while(line != null){
				centroid.add(convertToVector(line.toString()));
				line = str.readLine();
			}
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException{
			double minDistance = 0.0;
			Vector minVector = new DenseVector();
			String line = value.toString();
			String tmpStr = line.split("\t")[1];
			Vector pointVector = convertToVector(tmpStr);
			//The point belong to the centroid which is nearest to it.
			for(Vector vector: centroid){
				double distance = new EuclideanDistanceMeasure().distance(pointVector, vector);
				if(distance < minDistance || minDistance == 0.0){
					minDistance = distance;
					minVector = vector;
				}
			}
			outputKey.set(minVector.toString());
			outputValue.set(pointVector);
			context.write(outputKey, outputValue);
		}
		
	}
	
	/**
	 * Calculate the mean vector of each clusters as the new centroid
	 * @author cindyzhang
	 *
	 */
	public static class kmeansReducer 
		extends Reducer<Text, VectorWritable, NullWritable, Text>{
		DecimalFormat df = new DecimalFormat("#0.00000");
		Text outputValue = new Text();
		
		@Override
		public void reduce(Text key, Iterable<VectorWritable> values, Context context) 
				throws IOException, InterruptedException{
			System.out.println(key.toString());
			Vector tmp = null;
			double count = 0.0;
			for(VectorWritable vector: values){
				count = count + 1.0;
				if(tmp == null)
					tmp = vector.get();
				else
					tmp = tmp.plus(vector.get());
			}
			tmp = tmp.divide(count);
			StringBuffer result = new StringBuffer("");
			for(int i=0; i<tmp.size(); i++){
				double num = tmp.getQuick(i);
				result.append(df.format(num)+SEPERATE);
			}
			outputValue.set(result.toString());
			context.write(NullWritable.get(), outputValue);
		}
	}
	
	/**
	 * Convert a string to a mahout vector
	 * @param str
	 * @return
	 */
	public static Vector convertToVector(String str){
		String[] strPoint = str.split(SEPERATE);
		double[] numPoint = new double[strPoint.length];
		for(int i=0; i<strPoint.length;i++){
			numPoint[i]=Double.parseDouble(strPoint[i].trim());
		}
		return new DenseVector(numPoint);
	}
	
	public static void main(String args[]) 
			throws IOException, ClassNotFoundException, InterruptedException{	
		String inputPath = "/Users/cindyzhang/Desktop/vector.txt";
		String outputPath = "/Users/cindyzhang/Desktop/output";
		int count = 1;
		boolean farFlag = true;
		while(farFlag){
			
			Configuration conf = new Configuration();
			conf.set("centroidPath", outputPath+String.valueOf(count-1)+"/part-r-00000");
			Job job = new Job(conf, "KMeans");
			job.setJarByClass(kmeans.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(VectorWritable.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			job.setMapperClass(kmeansMapper.class);
			job.setReducerClass(kmeansReducer.class);
			job.setNumReduceTasks(1);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			FileInputFormat.addInputPath(job, new Path(inputPath));
			FileOutputFormat.setOutputPath(job, new Path(outputPath+String.valueOf(count)));
			job.waitForCompletion(true);
			farFlag = checkTwoCentroid(outputPath+String.valueOf(count-1), outputPath+String.valueOf(count));
			count++;
		}		
	}
	
	/**
	 * To check whether it converge
	 * When the distance between each pair of centroids is less than 0.01,
	 * believe that it's converged
	 * @param path1
	 * @param path2
	 * @return
	 * @throws IOException
	 */
	@SuppressWarnings({ "resource", "deprecation" })
	public static boolean checkTwoCentroid(String path1, String path2) throws IOException{
		DataInputStream str1 = new DataInputStream
				(new FileInputStream(path1+"/part-r-00000"));
		DataInputStream str2 = new DataInputStream
				(new FileInputStream(path2+"/part-r-00000"));
		String line1 = str1.readLine();
		String line2 = str2.readLine();
		double distance = 0.0;
		//System.out.println("----------------------");
		while(line1 != null && line2 != null){
			distance = new EuclideanDistanceMeasure().distance
				(convertToVector(line1.toString()),convertToVector(line2.toString()));
			//System.out.println(distance);
			line1 = str1.readLine();
			line2 = str2.readLine();
			if(distance > 0.01)
				return true;	
		}
		//System.out.println("----------------------");
		return false;
	}

}
