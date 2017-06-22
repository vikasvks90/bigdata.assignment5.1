/**
 * <h1>Task4</h1>
 * This class will be used to define the configuration components for the given task
 * */
package mapreduce.assignment5.task4;

import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class Task4 {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Task4");
		job.setJarByClass(Task4.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(Task4Mapper.class);
		job.setReducerClass(Task4Reducer.class);
		//number of partitioners equal to number of reducers
		job.setPartitionerClass(Task4Partitioner.class);
		//this will set the number of reducers equal to 4
		job.setNumReduceTasks(4);
		 
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0])); 
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		job.waitForCompletion(true);
	}
}