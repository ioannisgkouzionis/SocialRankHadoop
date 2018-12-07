package gr.tuc.softnet;

import java.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class DiffRed1 extends Reducer<Text, Text, Text, Text> 
{

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		// O user einai to node pou dinei o Mapper
		String user = key.toString();
		// Pinakas pou apothikeuei ta 2 ranks gia kathe node
		double[] ranks = new double[2];
		int i = 0;
		// Kanoume epanalipsi gia oles tis times, kai prepei na yparxoun
		// to poly 2 ranks gia kathe node
		for (Text v : values) 
		{	
			// Elegxos stin periptwsh pou yparxoun perissotera apo 2 ranks
			if (i < 2) 
			{
				// Apothikeuoume ta ranks
				ranks[i] = Double.parseDouble(v.toString()); 
			} 
			else 
			{
				// An yparxoun perissotera apo 2 ranks gia ena node petame exception
				throw new IOException("Yparxoun perissotera apo 2 rank times gia to idio node"); 
			}
			i++;
		}
		// Ypologismos ths diaforas twn 2 ranks gia kathe node
		double difference = Math.abs(ranks[0] - ranks[1]); 
		// Exodos sti morfi : kleidi: user timi: diafora twn 2 ranks
		context.write(new Text(user), new Text(difference + "")); 
	}
}
