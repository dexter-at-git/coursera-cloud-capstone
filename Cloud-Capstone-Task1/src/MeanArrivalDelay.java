import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MeanArrivalDelay extends Configured implements Tool {

	private static int CARRIER_POSITION = 2;
	private static int ORIGIN_POSITION = 3;
	private static int DESTINATION_POSITION = 4;
	private static int IS_DEPARTURE_DELAY_POSITION = 6;
	private static int ARRIVAL_DELAY_MINUTES_POSITION = 7;
	private static int IS_ARRIVAL_DELAY_POSITION = 8;
	private static int AIRLINE_ID_POSITION = 9;
	private static int TopNumber = 10;

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new MeanArrivalDelay(),
				args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		FileSystem fs = FileSystem.get(conf);
		Path outputPath = new Path(args[1]);
		fs.delete(outputPath, true);

		Job jobA = Job.getInstance(conf, "Origin-destination delay");
		jobA.setOutputKeyClass(Text.class);
		jobA.setOutputValueClass(FloatWritable.class);

		jobA.setMapperClass(OriginDestinationDelayMap.class);
		jobA.setReducerClass(OriginDestinationDelayReduce.class);

		FileInputFormat.setInputPaths(jobA, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobA, outputPath);

		jobA.setJarByClass(OnTimeAirlines.class);
		return jobA.waitForCompletion(true) ? 0 : 1;

	}

	public static class OriginDestinationDelayMap extends
			Mapper<Object, Text, Text, FloatWritable> {

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] values = value.toString().split("\\|");
			if (values.length < 10) {
				return;
			}
			String origin = values[ORIGIN_POSITION];
			String destination = values[DESTINATION_POSITION];
			String arrivalDelayString = values[ARRIVAL_DELAY_MINUTES_POSITION];
			if (origin.isEmpty() || destination.isEmpty()
					|| arrivalDelayString.isEmpty()) {
				return;
			}
			Float arrivalDelay = Float.parseFloat(arrivalDelayString);
			context.write(new Text(origin + "-" + destination),
					new FloatWritable(arrivalDelay));
		}
	}

	public static class OriginDestinationDelayReduce extends
			Reducer<Text, FloatWritable, Text, FloatWritable> {

		@Override
		public void reduce(Text key, Iterable<FloatWritable> values,
				Context context) throws IOException, InterruptedException {
			float totalMinutes = 0;
			int count = 0;
			for (FloatWritable val : values) {
				count++;
				totalMinutes += val.get();
			}
			Float averageMinutesDelay = totalMinutes / count;
			FloatWritable result = new FloatWritable(averageMinutesDelay);
			context.write(key, result);
		}
	}

}
