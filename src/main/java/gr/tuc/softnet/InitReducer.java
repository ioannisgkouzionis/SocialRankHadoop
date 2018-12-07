package gr.tuc.softnet;

import java.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class InitReducer extends Reducer<Text, Text, Text, Text> 
{
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		// Metatropi toy kleidoy(user) se String
		String user = key.toString();
		// Arxikopoiisi tis metavlitis rank = 1
		String rank = "1";
		// Ftiaxnoyme ena string ton Node kai rank sti morfi : node;rank
		//user = user + "-" + rank; 
		String friends = "";
		// Epanalipsi gia olous tous friends kathe user
		for (Text value : values) 
		{
			// Prosthetei kathe filo se mia akolouthia diaxorismeni me komma
			friends = friends + value.toString() + ","; 
		}
		// Diagrafoume to teleytaio komma apo tin akolouthia ton filon
		friends = friends.substring(0, friends.lastIndexOf(','));
		// H morfi tis exodou einai: node;rank friend1,friend2.....,friendn
		context.write(new Text(user+";"+rank), new Text(friends)); 
	}
}
