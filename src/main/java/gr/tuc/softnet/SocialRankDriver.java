package gr.tuc.softnet;

import java.io.*;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SocialRankDriver {

	public static void main(String[] args)throws Exception {
		String job = "";
		System.out.println("*** Author: Giannis Gkouzionis\n");
		// Elegxos kai ektelesh twn ergasiwn
		try {
			if (args.length != 0) 
				job = args[0];
		
			if (job.equals("init")) 
			{
				init(args[1], args[2], Integer.parseInt(args[3])); 
			} 
			else if (job.equals("iter")) 
			{
				iter(args[1], args[2], Integer.parseInt(args[3]));
			}
			else if (job.equals("diff")) 
			{
				diff(args[1], args[2], args[3], Integer.parseInt(args[4])); 
			}
			else if (job.equals("finish")) 
			{
				finish(args[1], args[2], Integer.parseInt(args[3]));
			}
			else if (job.equals("composite")) 
			{
				composite(args[1], args[2], args[3], args[4], args[5],
						Integer.parseInt(args[6]), Double.parseDouble(args[7]));
			}
		} 
		// Se periptwsi pou dothike lathos parametros emfanizoume minima me ti swstes entoles gia thn
		// ektelesh twn ergasiwn
		catch (Exception e) {			
			System.err.println("Lathos orismata twn entolwn \n " +
							"Swstos tropos grafhs twn entolwn:\n" +
							"SocialRankDriver init <inputDir> <outputDir> <#reducers>\n" +
							"SocialRankDriver iter <inputDir> <outputDir> <#reducers>\n" +
							"SocialRankDriver diff <inputDir1> <inputDir2> <outputDir> <#reducers>\n" +
							"SocialRankDriver finish <inputDir> <outputDir> <#reducers>\n" +
							"SocialRankDriver composite <inputDir> <outputDir> <interimDir1> <interimDir2> <diffDir> <#reducers> <#threshold>");
		}
	}

	static void init(String input, String output, int reducers)
			throws Exception 
	{
		System.out.println("Init Job Started\n");
		// Dimioyrgia enos neou Job
		Job job = Job.getInstance();
		// Thetoume tin klasi toy Driver
		job.setJarByClass(SocialRankDriver.class);
		// Thetoume ton aritmo ton reducers
		job.setNumReduceTasks(reducers);
		// Thetoume tis diadromes fakelon eisodou kai exodou
		FileInputFormat.addInputPath(job, new Path(input));
		// Diagrafi toy fakelloy exodoy
		deleteDirectory(output);		
		FileOutputFormat.setOutputPath(job, new Path(output));
		// Arxikopoiisi ton klaseon Mapper kai Reducer
		job.setMapperClass(InitMapper.class); 
		job.setReducerClass(InitReducer.class);
		// Thetoume ton typo eisodou exodou gia ton Mapper kai ton Reducer
		job.setMapOutputKeyClass(Text.class); 
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Ektypwsi minimatos gia ti swsti oloklirwsi 'h se periptwsi lathous 
		System.out.print(job.waitForCompletion(true) ? "Init Job Completed\n"
				: "Init Job Error\n");
	}

	static void iter(String input, String output, int reducers)
			throws Exception 
	{
		System.out.println("Iter Job Started\n");
		// Dimioyrgia enos neou Job
		Job job = Job.getInstance(); 
		// Thetoume tin klasi toy Driver
		job.setJarByClass(SocialRankDriver.class);
		// Thetoume ton aritmo ton reducers
		job.setNumReduceTasks(reducers);
		// Thetoume tis diadromes fakelon eisodou
		FileInputFormat.addInputPath(job, new Path(input)); 
		// Diagrafi toy fakelloy exodoy
		deleteDirectory(output);
		// Thetoume tis diadromes fakelon exodou
		FileOutputFormat.setOutputPath(job, new Path(output));
		// Arxikopoiisi ton klaseon Mapper kai Reducer
		job.setMapperClass(IterMapper.class); 								
		job.setReducerClass(IterReducer.class);
		// Thetoume ton typo eisodou exodou gia ton Mapper kai ton Reducer
		job.setMapOutputKeyClass(Text.class); 
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Ektypwsi minimatos gia ti swsti oloklirwsi 'h se periptwsi lathous
		System.out.print(job.waitForCompletion(true) ? "Iter Job Completed\n"
				: "Iter Job Error\n");

	}

	static void diff(String input1, String input2, String output, int reducers)
			throws Exception 
	{
		System.out.println("Diff Job Part 1 Started\n");
		String tempdiffPath = "tempdiff";
		// Dimioyrgia enos neou Job
		Job job = Job.getInstance(); 
		// Thetoume tin klasi toy Driver
		job.setJarByClass(SocialRankDriver.class);
		// Thetoume ton aritmo ton reducers
		job.setNumReduceTasks(reducers);
		// Thetoume tis diadromes fakelon eisodou1
		FileInputFormat.addInputPath(job, new Path(input1)); 
		// Thetoume tis diadromes fakelon eisodou2
		FileInputFormat.addInputPath(job, new Path(input2));
		// Diagrafi toy temporary path
		deleteDirectory(tempdiffPath);
		// Thetoume enan prosorino fakelo eksodou tempdiff
		FileOutputFormat.setOutputPath(job, new Path(tempdiffPath)); 
		// Arxikopoiisi ton klaseon Mapper kai Reducer gia to 1o job
		job.setMapperClass(DiffMap1.class); 
		job.setReducerClass(DiffRed1.class);
		// Thetoume ton typo eisodou exodou gia ton Mapper kai ton Reducer tou job 1
		job.setMapOutputKeyClass(Text.class); 
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// Otan oloklirwthei to 1o job
		if (job.waitForCompletion(true)) 
		{
			System.out.println("Diff Part 1 Complete, Part 2 Started\n");
			// Dimiourgia enos neou 2ou job
			Job job1 = Job.getInstance(); 
			// Thetoume tin klasi toy Driver
			job1.setJarByClass(SocialRankDriver.class); 
			// Thetoume ton aritmo ton reducers
			job1.setNumReduceTasks(reducers);
			// Thetoume ton proswrino fakelo tempdiff ws eisodo
			FileInputFormat.addInputPath(job1, new Path(tempdiffPath)); 
			// Diagrafi toy fakelloy exodoy
			deleteDirectory(output);
			// Thetoume tin eksodo
			FileOutputFormat.setOutputPath(job1, new Path(output)); 
			// Thetoume ton typo eisodou gia ton Mapper kai ton Reducer tou job 2
			job1.setMapperClass(DiffMap2.class); 
			job1.setReducerClass(DiffRed2.class);
			// Thetoume ton typo exodou gia ton Mapper kai ton Reducer tou job 2
			job1.setMapOutputKeyClass(DoubleWritable.class); 
			job1.setMapOutputValueClass(Text.class);

			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(Text.class);

			// Ektypwsi minimatos gia ti swsti oloklirwsi 'h se periptwsi lathous 
			System.out.print(job1.waitForCompletion(true) ? "Diff Job Completed\n"
							: "Diff Job Error\n");
			// Diagrafoume ton proswrino fakelo tempdiff
			deleteDirectory(tempdiffPath); 
		}
	}

	static void finish(String input, String output, int reducers)
			throws Exception 
	{
		System.out.println("Finish Job Started\n");
		// Dimioyrgia enos neou Job
		Job job = Job.getInstance(); 
		// Thetoume tin klasi toy Driver
		job.setJarByClass(SocialRankDriver.class); 
		// Thetoume ton aritmo ton reducers
		job.setNumReduceTasks(reducers); 
		// Thetoume tis diadromes fakelon eisodou kai exodou
		FileInputFormat.addInputPath(job, new Path(input));
		// Diagrafi toy fakelloy exodoy
		deleteDirectory(output);
		FileOutputFormat.setOutputPath(job, new Path(output));
		// Arxikopoiisi ton klaseon Mapper kai Reducer
		job.setMapperClass(FinMapper.class); 
		job.setReducerClass(FinReducer.class);
		// Thetoume ton typo eisodou exodou gia ton Mapper kai ton Reducer
		job.setMapOutputKeyClass(DoubleWritable.class); 
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Ektypwsi minimatos gia ti swsti oloklirwsi 'h se periptwsi lathous
		System.out.print(job.waitForCompletion(true) ? "Finish Job Completed\n"
				: "Finish Job Error\n");
	}

	public static void composite(String input, String output, String interim1,
			String interim2, String diff, int reducers, double threshold) throws Exception 
	{
		System.out.println("SocialRankAlgorithm (tuc)\n");
		int counter = 0;
		// Arxikopoiisi dedomenon
		init(input, interim1, reducers); 
		counter++;
		// Thetoume mia poly megali timi gia tin diafora
		double difference = 100000000; 
		// Metavlites metrites
		int i = 0; 
		// H diadikasia epanalamvanetai ews otou h diafora metaksy twn 2 ranks
		// na einai mikroteri apo to epipedo pou thetoume(threshold)
		while (difference >= threshold) 
		{
			if (i % 2 == 0) 
			{
				// Gia kathe artio arithmo epanalipsewn, to interim1 einai i eisodos kai to
				// interim2 einai i eksodos
				iter(interim1, interim2, reducers); 
			} 
			else 
			{
				// Gia tis perittes epanalipseis exoume to anapodo gia eisodo
				iter(interim2, interim1, reducers); 
			}
			// Gia kathe 3 epanalipseis ypologizoume tin diafora metaksi twn 2 ranks
			if (i % 3 == 0) 
			{
				counter++;
				// Oso i diafora einai mia apoliti timi, i seira twn input1
				// input2 monopatiwn den exei simasia
				diff(interim1, interim2, diff, reducers); 
				// Diavazoume tin diafora apo to diffout
				difference = readDiffResult(diff); 
				System.out.println("Difference updates to:" + difference + "\n");
				// Diagrafoume to directory diffout
				deleteDirectory(diff); 
			}
			// Stis arties epanalipseis, diagrafoume to monopati interim1
			if (i % 2 == 0) 
			{
				deleteDirectory(interim1);
			}
			// Stis perittes epanalipseis, diagrafoume to monopati interim2
			if (i % 2 == 1) 
			{
				deleteDirectory(interim2);
			}
			counter++;
			// Auksanoume ton metriti i
			i++; 
		}
		// Oso to i auksanetai sto teleutaio vima, gia peritta i, to interim2 einai to monopati eisodou
		if (i % 2 == 1) 
		{
			// Diagrafoume ta alla monopatia
			deleteDirectory(interim1); 
			counter++;
			finish(interim2, output, reducers);
		} 
		// Gia artio i, to interim1 einai to monopati eisodou
		else 
		{
			// Diagrafoume ta alla monopatia
			deleteDirectory(interim2); 
			counter++;
			finish(interim1, output, reducers);
		}
		System.out.println();
		System.out.println(counter);
	}

	// Dedomenou enos fakelou eksodou, epistrefoume tin 1i double timi 
	// apo to 1o part-r-00000 arxeio
	// Ayti h timi einai i megalyteri diafora giati to arxeio diff
	// einai taxinomimeno me fthinousa seira opote an to threshold einai
	// megalytero apo ayti tin timi stamataei i ektelesi tis ergasias
	static double readDiffResult(String path) throws Exception 
	{
		double diffnum = 0.0;
		// Orizw mia metavliti i opoia periexei tin diadromi mesa sto sistima tou HDFS gia to fakelo eksodou tis diff
		Path diffpath = new Path(path);
		// To Configuration periexei oles tis aparaitites rithmiseis tou Hadoop gia na treksei i efarmogi
		Configuration conf = new Configuration();
		// 
		FileSystem fs = FileSystem.get(URI.create(path), conf);

		if (fs.exists(diffpath)) 
		{
			// O pinakas ls periexei tis diadromes twn arxeiwn pou vriskontai sto diffpath (dld sto fakelo eksodou tis diff)
			FileStatus[] ls = fs.listStatus(diffpath);
			// Anoigw kathe arxeio pou vrisketai ston parapanw pinaka wste na vrw tin megaliteri diafora ranks
			for (FileStatus file : ls) 
			{
				// Elegxw an to onoma tou trexontos arxeio ksekinaei me "part-r-00"
				if (file.getPath().getName().startsWith("part-r-00")) 
				{
					// Fortwnw ta periexomena tou arxeiou se ena stream
					FSDataInputStream diffin = fs.open(file.getPath());
					BufferedReader d = new BufferedReader(new InputStreamReader(diffin));
					// Diavazw tin 1i grammi tou arxeiou i opoia periexei tin megaliteri diafora (sto arxeio auto)
					String diffcontent = d.readLine();
					if (diffcontent != null) 
					{
						double diff_temp = Double.parseDouble(diffcontent);
						// Elegxw kai krataw panta tin megaliteri timi (diafora) apo kathe arxeio mexri na vrw tin megaliteri ap ola ta arxeia 
						if (diffnum < diff_temp) 
						{
							diffnum = diff_temp;
						}
						d.close();
					}
				}
			}
		}

		fs.close();
		// Epistrefw tin megaliteri diafora
		return diffnum;
	}

	static void deleteDirectory(String path) throws Exception 
	{
		Path todelete = new Path(path);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(path), conf);

		if (fs.exists(todelete))
			fs.delete(todelete, true);

		fs.close();
	}

}
