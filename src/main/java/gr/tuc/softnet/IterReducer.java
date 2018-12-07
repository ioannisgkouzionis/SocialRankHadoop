package gr.tuc.softnet;

import java.io.*;
//import java.util.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class IterReducer extends Reducer<Text, Text, Text, Text> 
{
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		// Edo orizoyme tin stathera d
		double d = 0.15;
		// H akoloythia ton filon se string
		String friend_list = "";
		double sum_weight = 0.0;
		// Epanalipsi gia oles tis times toy arxeioy eisodoy
		for (Text v : values) 
		{
			// An den vrethei o endeiktikos xaraktiras '~' ypologismos toy rank
			// allios prosthiki stin akoloythia ton filon
			if (v.find("~", 0)<0) 
			{
				// Apothikeysi toy weight se mia metavliti double
				double weight = Double.parseDouble(v.toString());
				// Ypologismos toy rank epanaliptika
				sum_weight = sum_weight + (weight); 
			} 
			else 
			{
				// I akolouthia ton filon se string me morfi friend1,friend2,....,friendn
				friend_list += (v.toString()).substring(1) + ",";
			}
		}
		// Diagrafoume to teleytaio komma apo tin akolouthia ton filon
		if (friend_list.length()>0) {
			friend_list = friend_list.substring(0, friend_list.lastIndexOf(','));
		}
		// Ektelesi toy algorithmoy
		double newRank = d + (1 - d) * sum_weight;
		// H morfi tis exodou einai: node;rank friend1,friend2.....,friendn
		context.write(new Text(key.toString() + ";" + newRank), new Text(friend_list)); 
	}
}
