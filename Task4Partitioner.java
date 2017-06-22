/**
 * <h1>Task4Partitioner</h1>
 * Partitioner program to which will get the output of mapper as input and will ensure that a particular
 * key goes to a particular reducer
 * */
package mapreduce.assignment5.task4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class Task4Partitioner extends Partitioner<Text, IntWritable> {

	@Override
	public int getPartition(Text key, IntWritable value, int numPartitions) {
			String word = key.toString();
			//getting the first letter of company name
			char letter = word.toLowerCase().charAt(0);
			int partitionNumber = 0;
			//company name starting with A-F (upper or lower case) will go to 1st reducer
			if(letter == 'a' || letter == 'b' || letter == 'c' || letter == 'd' || letter == 'e' || letter == 'f'){
				partitionNumber = 0;
			}
			//company name starting with G-L (upper or lower case) will go to 2nd reducer
			else if(letter == 'g' || letter == 'h' || letter == 'i' || letter == 'j' || letter == 'k' || letter == 'l'){
				partitionNumber = 1;
			}
			//company name starting with M-R (upper or lower case) will go to 3rd reducer
			else if(letter == 'm' || letter == 'n' || letter == 'o' || letter == 'p' || letter == 'q' || letter == 'r'){
				partitionNumber = 2;
			}
			//all other company name other than that will go to 4th reducer
			else{
				partitionNumber = 3;
			}
			return partitionNumber;
	}
}