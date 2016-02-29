import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
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

public class MostPopularAirports extends Configured implements Tool {

	private static int CARRIER_POSITION = 2;
	private static int ORIGIN_POSITION = 3;
	private static int DESTINATION_POSITION = 4;
	private static int DEPARTURE_DELAY_POSITION = 6;
	private static int ARRIVAL_DELAY_POSITION = 8;
	private static int AIRLINE_ID_POSITION = 9;
	private static int TopNumber = 100000000;

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new MostPopularAirports(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		FileSystem fs = FileSystem.get(conf);
		Path tmpPath = new Path("/user/capstone/tmp");
		Path outputPath = new Path(args[1]);
		fs.delete(outputPath, true);
		fs.delete(tmpPath, true);

		Job jobA = Job.getInstance(conf, "Airports Count");
		jobA.setOutputKeyClass(Text.class);
		jobA.setOutputValueClass(IntWritable.class);

		jobA.setMapperClass(AirportCountMap.class);
		jobA.setReducerClass(AirportCountReduce.class);

		FileInputFormat.setInputPaths(jobA, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobA, tmpPath);

		jobA.setJarByClass(MostPopularAirports.class);
		jobA.waitForCompletion(true);

		Job jobB = Job.getInstance(conf, "Top Airports");
		jobB.setOutputKeyClass(Text.class);
		jobB.setOutputValueClass(IntWritable.class);

		jobB.setMapOutputKeyClass(NullWritable.class);
		jobB.setMapOutputValueClass(TextArrayWritable.class);

		jobB.setMapperClass(TopAirportsMap.class);
		jobB.setReducerClass(TopAirportsReduce.class);
		jobB.setNumReduceTasks(1);

		FileInputFormat.setInputPaths(jobB, tmpPath);
		FileOutputFormat.setOutputPath(jobB, outputPath);

		jobB.setInputFormatClass(KeyValueTextInputFormat.class);
		jobB.setOutputFormatClass(TextOutputFormat.class);

		jobB.setJarByClass(MostPopularAirports.class);
		return jobB.waitForCompletion(true) ? 0 : 1;
	}

	public static class AirportCountMap extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] values = value.toString().split("\\|");
			if (values.length < 10) {
				return;
			}
			String originAirport = values[ORIGIN_POSITION];
			String destinationAirport = values[DESTINATION_POSITION];
			if (originAirport.isEmpty() || destinationAirport.isEmpty()) {
				return;
			}

			context.write(new Text(originAirport), one);
			context.write(new Text(destinationAirport), one);
		}
	}

	public static class AirportCountReduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable result = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static class TopAirportsMap extends
			Mapper<Text, Text, NullWritable, TextArrayWritable> {

		private TreeSet<Pair<Integer, String>> flightsCountToAirportMap = new TreeSet<Pair<Integer, String>>(
				Collections.reverseOrder());
		private int N;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			this.N = conf.getInt("N", Integer.MAX_VALUE);
		}

		@Override
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			Integer flightsCount = Integer.parseInt(value.toString());
			String airport = key.toString();
			flightsCountToAirportMap.add(new Pair<Integer, String>(
					flightsCount, airport));
			if (flightsCountToAirportMap.size() > N) {
				flightsCountToAirportMap.remove(flightsCountToAirportMap
						.last());
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			for (Pair<Integer, String> item : flightsCountToAirportMap) {
				String[] strings = { item.second, item.first.toString() };
				TextArrayWritable val = new TextArrayWritable(strings);
				context.write(NullWritable.get(), val);
			}
		}
	}

	public static class TopAirportsReduce extends
			Reducer<NullWritable, TextArrayWritable, Text, IntWritable> {

		private TreeSet<Pair<Integer, String>> flightsCountToAirportMap = new TreeSet<Pair<Integer, String>>(
				Collections.reverseOrder());
		private int N;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			this.N = conf.getInt("N", Integer.MAX_VALUE);
		}

		@Override
		public void reduce(NullWritable key,
				Iterable<TextArrayWritable> values, Context context)
				throws IOException, InterruptedException {
			for (TextArrayWritable val : values) {
				Text[] pair = (Text[]) val.toArray();
				String airport = pair[0].toString();
				Integer flightsCount = Integer.parseInt(pair[1].toString());
				flightsCountToAirportMap.add(new Pair<Integer, String>(
						flightsCount, airport));
				if (flightsCountToAirportMap.size() > N) {
					flightsCountToAirportMap.remove(flightsCountToAirportMap
							.last());
				}
			}
			for (Pair<Integer, String> item : flightsCountToAirportMap) {
				Text word = new Text(item.second);
				IntWritable value = new IntWritable(item.first);
				context.write(word, value);
			}
		}
	}

	private static class TextArrayWritable extends ArrayWritable {
		public TextArrayWritable() {
			super(Text.class);
		}

		public TextArrayWritable(String[] strings) {
			super(Text.class);
			Text[] texts = new Text[strings.length];
			for (int i = 0; i < strings.length; i++) {
				texts[i] = new Text(strings[i]);
			}
			set(texts);
		}
	}

	private static class Pair<A extends Comparable<? super A>, B extends Comparable<? super B>>
			implements Comparable<Pair<A, B>> {

		public final A first;
		public final B second;

		public Pair(A first, B second) {
			this.first = first;
			this.second = second;
		}

		public static <A extends Comparable<? super A>, B extends Comparable<? super B>> Pair<A, B> of(
				A first, B second) {
			return new Pair<A, B>(first, second);
		}

		@Override
		public int compareTo(Pair<A, B> o) {
			int cmp = o == null ? 1 : (this.first).compareTo(o.first);
			return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
		}

		@Override
		public int hashCode() {
			return 31 * hashcode(first) + hashcode(second);
		}

		private static int hashcode(Object o) {
			return o == null ? 0 : o.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (!(obj instanceof Pair))
				return false;
			if (this == obj)
				return true;
			return equal(first, ((Pair<?, ?>) obj).first)
					&& equal(second, ((Pair<?, ?>) obj).second);
		}

		private boolean equal(Object o1, Object o2) {
			return o1 == o2 || (o1 != null && o1.equals(o2));
		}

		@Override
		public String toString() {
			return "(" + first + ", " + second + ')';
		}
	}
}
