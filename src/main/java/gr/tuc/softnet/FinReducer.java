package gr.tuc.softnet;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FinReducer extends Reducer<DoubleWritable, Text, Text, Text> 
{
	public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException,
			InterruptedException 
	{
		// Metatropi tou rank se thetiko
		Double rank = -Double.parseDouble(key.toString());
		// Epanalipsi gia oles tis times
		for (Text v : values) 
		{
			// Exodos sti morfi: kleidi: node timi: rank
			context.write(v, new Text(rank + "")); 
		}
	}
}
