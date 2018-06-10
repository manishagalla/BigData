

package BigData;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.Map.Entry;

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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Sidejoin1 {
	public static class Business extends Mapper<LongWritable, Text, Text, Text>{
		private Text bid= new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String[] line =value.toString().split("::");
			bid.set(line[0]);
			context.write(bid, new Text(bid+","+line[1].trim()+","+line[2].trim()));
		}
	}
	
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
	
	public static class MapTopRated extends Mapper<LongWritable, Text, Text,Text>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			    Text busnsId= new Text();
			String[] line = value.toString().split("\t");
				busnsId.set(line[0]);
				context.write(busnsId,new Text(busnsId+","+line[1]));
			}
		}
	public static class Business_Reduce extends Reducer<Text,Text,Text,Text>{
		ArrayList<String> topten= new ArrayList<>();
		ArrayList<String> business= new ArrayList<>();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			for (Text text : values) {
				String value = text.toString();
				String[] lines= text.toString().split(",");
				if (lines.length<3) {
					topten.add(value);
				} else {
					business.add(value);
				}
			}
		}
	
		@Override
		protected void cleanup(Context context) throws IOException,
		InterruptedException {
	for (String item : topten) {
		for (String item1 : business) {
			String[] map2 = item.split(",");
			String m2bid = map2[0].trim();

			String[] map3 = item1.split(",");
			String m3bid = map3[0].trim();

			if (m2bid.equals(m3bid)) {
				context.write(new Text(m2bid), new Text(
						map3[1] + "\t" + map3[2] + "\t"
								+ map2[1]));
				System.out.println("wrote");
				break;
			}
		}
	}
}
}
	public static void main(String[] args) throws Exception {
		Configuration config = new Configuration();
		String[] otherArgs = new GenericOptionsParser(config, args)
				.getRemainingArgs();
		Job job1 = Job.getInstance(config, "job1");
		job1.setJarByClass(Sidejoin1.class);
		job1.setMapperClass(Map.class);
		job1.setReducerClass(Reduce.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(FloatWritable.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[2]));

		boolean isJob1Completed = job1.waitForCompletion(true);

		if (isJob1Completed) {
			Configuration config2 = new Configuration();
			Job job2 = Job.getInstance(config2, "job2");
			job2.setJarByClass(Sidejoin1.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			job2.setInputFormatClass(TextInputFormat.class);
			job2.setOutputFormatClass(TextOutputFormat.class);
			MultipleInputs.addInputPath(job2, new Path(args[2]),
					TextInputFormat.class, MapTopRated.class);
			MultipleInputs.addInputPath(job2, new Path(args[1]),
					TextInputFormat.class, Business.class);
			job2.setReducerClass(Business_Reduce.class);
			FileOutputFormat.setOutputPath(job2, new Path(args[3]));

			job2.waitForCompletion(true);
		}
	}

}


