package BigData;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.List;
import java.util.NavigableSet;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Rating {
	public static class Map extends Mapper<LongWritable, Text, Text, FloatWritable>{
		private final static IntWritable one = new IntWritable(1);
		private Text busnsId = new Text(); // type of output key
		private FloatWritable rating= new FloatWritable();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split("::");
				busnsId.set(line[2]);
				float r= Float.parseFloat(line[3]);
				rating.set(r);
				context.write(busnsId,rating);
			
		}
		
	}
	
	
	public static class Reduce extends Reducer<Text,FloatWritable,Text,FloatWritable> {
		TreeMap<Float, List<Text>> list = new TreeMap<Float, List<Text>>(Collections.reverseOrder());
		public void reduce(Text key, Iterable<FloatWritable> values,Context context) throws IOException, InterruptedException {
			float sum = 0;
			int count=0;
			for(FloatWritable item:values){
				sum+=item.get();
				count++;
			}
			Float avg=(float)sum/count;
			if(list.containsKey(avg))
			{
			list.get(avg).add(new Text(key.toString()));
			}
			else
			{
			List<Text> bId_List = new ArrayList<Text>();
			bId_List.add(new Text(key.toString()));
			list.put(avg, bId_List); 
			}
			}
			
	@Override
	protected void cleanup(Reducer<Text, FloatWritable, Text, FloatWritable>.Context context)throws IOException, InterruptedException 
	{
		int count = 0;
		List<Entry<Float, List<Text>>> list1= new ArrayList<Entry<Float, List<Text>>>(list.entrySet());
        for(Entry<Float, List<Text>> item: list1){
        	Float key= item.getKey();
        	List<Text> bid_list = list.get(key);
        	for(Text t : bid_list)
        	{
        	 count++;
        	 if(count<=10) 
	         context.write(new Text(t.toString()), new FloatWritable(key));
        	}
        	}

	}
	}

	public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	Job job = new Job(conf, "rating");
	job.setJarByClass(Rating.class);
	job.setMapperClass(Map.class);
	job.setReducerClass(Reduce.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(FloatWritable.class);
	FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
