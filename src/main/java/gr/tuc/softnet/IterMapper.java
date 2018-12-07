package gr.tuc.softnet;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class IterMapper extends Mapper<LongWritable, Text, Text, Text> 
{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
		// Metatropi tis grammis se String
		String line = value.toString(); 
		// Diaxorismos tis grammis se 2 meri. Meros 1: node;rank kai Meros 2: friend1,friend2...,friendn
		String[] user_parts = line.split("\t");
		// Elegxoume an o user exei friends
		if (user_parts.length < 2) 
		{
			return;
		}
		// Diaxorismos tis akolouthias ton friends (friend1,friend2...,friendn) se kathe ena xexoristo friend
		// kai apothikeysi toys se ena pinaka
		String[] friends = user_parts[1].split(","); 
		// Diaxorismos toy node;rank kai apothikeysi toys se ena pinaka
		String[] user_rank = user_parts[0].split(";");
		// Apothikeysi toy node se mia metavliti string
		String user = user_rank[0];
		// Apothikeysi toy rank se mia metavliti double
		double rank = Double.parseDouble(user_rank[1]);
		// O arithmos ton friends
		double friendsNumber = friends.length; 
		// rankWeight ana epanalipsi toy SocialRank algorithmoy
		String rankWeight = Double.toString(rank / friendsNumber); 
		// Paragogi zeygarion friend - rankWeight: klidi: friendn timi: rankWeight
		for (String friend : friends) 
		{
			context.write(new Text(friend), new Text(rankWeight));
			// Exodos sti morfi klidi: node Timi: ~friendn opou to '~' einai endeiktiko toy teloys tis kathe akmis
			context.write(new Text(user), new Text("~"+friend));
		} 
	}
}
