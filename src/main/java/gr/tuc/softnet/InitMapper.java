package gr.tuc.softnet;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class InitMapper extends Mapper<LongWritable, Text, Text, Text> 
{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,
	IllegalArgumentException 
	{
		// Metatropi tis grammis se String
		String line = value.toString();
		if (line.contains("\t")) 
		{
			// Diaxorismos tis grammis se 2 meri
			String[] parts = line.split("\t"); 
			// Elegxo oti ta 2 meri apotelounte mono apo arithmous
			if (parts[0].matches("[0-9]+") && parts[1].matches("[0-9]+")) 
			{ 
				// Vgazoume ta zeygi kleidi - timi sti morfi xristis - filos
				context.write(new Text(parts[0]), new Text(parts[1])); 
			} 
			else 
			{
				// Lathos an yparxoyn alloi xaraktires ektos apo arithmous
				throw new IllegalArgumentException("Kapoio apo ta 2 meri den periexei mono arithmous"); 
			}
		}
		else 
		{
			// Lathos stin morfi toy arxeioy eisodou
			throw new IllegalArgumentException("Ta dedomena sto arxeio eisodou exoun lathos morfi"); 
		}
	}
}
