/**
 * <h1>Task5Reducer</h1>
 * Reducer program to calculate the total units sold for each Company
 * This class will take input as (Key,Value) pair from output of mapper class
 * value will be a combined list for all the values for a given key
 * */
package mapreduce.assignment5.task5;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task5Reducer extends Reducer<Text, IntWritable, Text, IntWritable>
{
	IntWritable outValue = new IntWritable();
	
	public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
	{
		//we are taking the value of mapper class which is (1) and adding up all the values
		//to get the total units sold for each company
		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		
		//output of reducer is total number of units sold for each company
		outValue.set(sum);
		context.write(key, outValue);
	}
}
