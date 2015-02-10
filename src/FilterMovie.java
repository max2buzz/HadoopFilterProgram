import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class FilterMovie {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			// Get the Genre Data from the context
			Configuration conf = context.getConfiguration();
			String genre = conf.get("genre");

			String line = value.toString();

			String fields[] = line.split("::"); // SPliting based on a
												// Delimeter.

			if (fields.length > 1) {
				if (fields[2].contains(genre)) {
					context.write(new Text(fields[1]), new Text(""));
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		if (otherArgs.length != 3) { //Three Args <in> <out> <genre>
			System.err
					.println("Program Error in Main: Fewer Arguments were Passed "
							+ "USAGE: Main <in> <out> <genre>");
			System.exit(2);
		}
		conf.set("genre", otherArgs[2].trim()); //The third argument is the Genre

		Job job = new Job(conf, "FilterMe");
		
		job.setJarByClass(FilterMovie.class);

		job.setMapperClass(Map.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setNumReduceTasks(0);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
