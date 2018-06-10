
package BigData;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.*;
//import org.apache.hadoop.fs.URI;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
public class InMemory{
public static class Business extends Mapper<LongWritable, Text, Text, NullWritable> {

	private Set<String> List = new HashSet<String>();
	@SuppressWarnings("deprecation")
	protected void setup(Context context) throws java.io.IOException,
			InterruptedException {

		try {
             
 			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			URI[] cacheFiles = context.getCacheFiles();
			Path getPath = new Path(cacheFiles[0].getPath());
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(getPath)));
 			int count=0;
 			String line="";
 			while ((line = br.readLine()) != null) {
				String[] lines = line.toString().split("::");
				
				if(lines[1].contains("Stanford")){
					count++;
					List.add(lines[0]);
					System.out.println(count);
				}
			}
 			}	
		 catch (IOException e) {
			System.err.println("Exception reading input file: " + e);

		}

	}

	

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] lines = value.toString().split("::");
		StringTokenizer tokenizer = new StringTokenizer(lines[2]);

		while (tokenizer.hasMoreTokens()) {
			String token = tokenizer.nextToken();
			if (List.contains(token)) {
				context.write(new Text(lines[1]+"\t"+lines[3]), null);
			} 
		}
	}
}
@SuppressWarnings("deprecation")

public static void main(String[] args) throws Exception{
Configuration conf = new Configuration();
String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
Job job = new Job(conf,"User rating");
job.setJarByClass(InMemory.class);
job.setMapperClass(Business.class);
job.setNumReduceTasks(0);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(Text.class);
FileInputFormat.addInputPath(job, new Path(otherArgs[1]));

FileSystem fs = FileSystem.get(conf);
if (fs.exists(new Path(args[2]))) {
	fs.delete(new Path(args[2]), true);
}
job.addCacheFile(new Path(args[0]).toUri());
FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}

