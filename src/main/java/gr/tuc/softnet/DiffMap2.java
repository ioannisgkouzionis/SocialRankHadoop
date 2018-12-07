package gr.tuc.softnet;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class DiffMap2 extends Mapper<LongWritable, Text, DoubleWritable, Text> 
{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,
			IllegalArgumentException 
	{
		// Metatropi tis grammis se string
		String s = value.toString();
		// Apothikeuw se ena pinaka ton user kai tin diafora ranks pou antistoixei se auton
		String[] userDiff = s.split("\t");
		// Pairnoume tin diafora twn ranks
		double diff = Double.parseDouble(userDiff[1]);
		// Exodos sti morfi: kleidi: diafora me arnhtiko proshmo wste to Hadoop na ta taksinomisei
		// me fthinousa seira timi: keno
		context.write(new DoubleWritable(-1.0*diff), new Text("")); 
	}
}
