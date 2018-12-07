package gr.tuc.softnet;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class DiffMap1 extends Mapper<LongWritable, Text, Text, Text> 
{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,
			IllegalArgumentException 
	{
		// Metatropi tis grammis se String
		String line = value.toString(); 
		// Diaxorismos tis grammis se 2 meri.
		String[] userRanks = line.split("\t");
		// Diaxorismos toy node;rank kai apothikeysi toys se ena pinaka
		String[] user_rank = userRanks[0].split(";");
		// Exodos sti morfi klidi: node Timi: rank
		context.write(new Text(user_rank[0]), new Text(user_rank[1])); 
	}
}
