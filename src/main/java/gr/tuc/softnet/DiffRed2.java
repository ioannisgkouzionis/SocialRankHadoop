package gr.tuc.softnet;

import java.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class DiffRed2 extends Reducer<DoubleWritable, Text, Text, Text> 
{
	public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		double diff_max = key.get(); 
		// Grafoume stin exodo tin diafora afou prwta tin metatrepsoume se thetiki (kathws eixame valei
		// arnhtiko proshmo ston DiffMap2)
		context.write(new Text(Double.toString(-1.0*diff_max)), new Text("")); 
	}
}
