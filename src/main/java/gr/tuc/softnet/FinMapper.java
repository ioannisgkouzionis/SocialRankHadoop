package gr.tuc.softnet;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class FinMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> 
{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,
			IllegalArgumentException 
	{
		// Metatropi tis grammis se string
		String line = value.toString(); 
		// Diaxorismos tis grammis se 2 meri
		String[] sections = line.split("\t");
		// Elegxos gia lathos morfh
		if (sections.length > 2) 
		{
			throw new IOException("Lathos morfh dedomenwn\n");
		}
		// Diaxorismos tou node;rank pinaka
		String[] parts = sections[0].split(";");
		Double rank = Double.parseDouble(parts[1]);
		// Exodos sti morfi: kleidi: to rank me arnhtiko proshmo wste to Hadoop na ta taksinomisei
		// me fthinousa seira timi: to node
		context.write(new DoubleWritable(-rank), new Text(parts[0])); 
	}
}