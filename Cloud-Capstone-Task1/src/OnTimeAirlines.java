import java.io.IOException;
import java.util.Date;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.FloatWritable;
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

public class OnTimeAirlines extends Configured implements Tool {

	private static int CARRIER_POSITION = 2;
	private static int ORIGIN_POSITION = 3;
	private static int DESTINATION_POSITION = 4;
	private static int IS_DEPARTURE_DELAY_POSITION = 6;
	private static int ARRIVAL_DELAY_MINUTES_POSITION = 7;
	private static int IS_ARRIVAL_DELAY_POSITION = 8;
	private static int AIRLINE_ID_POSITION = 9;
	private static int AIRLINE_POSITION = 10;
	private static int TopNumber = 10;

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new OnTimeAirlines(),
				args);
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

		Job jobA = Job.getInstance(conf, "Airlines delay");
		jobA.setOutputKeyClass(Text.class);
		jobA.setOutputValueClass(FloatWritable.class);

		jobA.setMapperClass(AirportCountMap.class);
		jobA.setReducerClass(AirportCountReduce.class);

		FileInputFormat.setInputPaths(jobA, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobA, tmpPath);

		jobA.setJarByClass(OnTimeAirlines.class);
		jobA.waitForCompletion(true);

		Job jobB = Job.getInstance(conf, "Top minimum delay");
		jobB.setOutputKeyClass(Text.class);
		jobB.setOutputValueClass(FloatWritable.class);

		jobB.setMapOutputKeyClass(NullWritable.class);
		jobB.setMapOutputValueClass(TextArrayWritable.class);

		jobB.setMapperClass(TopMinDelayMap.class);
		jobB.setReducerClass(TopMinDelayReduce.class);
		jobB.setNumReduceTasks(1);

		FileInputFormat.setInputPaths(jobB, tmpPath);
		FileOutputFormat.setOutputPath(jobB, outputPath);

		jobB.setInputFormatClass(KeyValueTextInputFormat.class);
		jobB.setOutputFormatClass(TextOutputFormat.class);

		jobB.setJarByClass(OnTimeAirlines.class);
		return jobB.waitForCompletion(true) ? 0 : 1;
	}

	public static class AirportCountMap extends
			Mapper<Object, Text, Text, FloatWritable> {

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] values = value.toString().split("\\|");
			if (values.length < 10) {
				return;
			}
			String airline = values[AIRLINE_ID_POSITION];
			String airlineName = values[AIRLINE_POSITION];
			String arrivalDelayString = values[ARRIVAL_DELAY_MINUTES_POSITION];
			if (airline.isEmpty() || arrivalDelayString.isEmpty()) {
				return;
			}
			Float arrivalDelay = Float.parseFloat(arrivalDelayString);
			context.write(new Text(airlineName + "(" + airline + ")"), new FloatWritable(arrivalDelay));
		}
	}

	public static class AirportCountReduce extends
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

	public static class TopMinDelayMap extends
			Mapper<Text, Text, NullWritable, TextArrayWritable> {

		private TreeSet<Pair<Float, String>> minutesDelayToCarrierMap = new TreeSet<Pair<Float, String>>();

		@Override
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			Float avgMinutesDelay = Float.parseFloat(value.toString());
			String airline = key.toString();
			minutesDelayToCarrierMap.add(new Pair<Float, String>(
					avgMinutesDelay, airline));
			if (minutesDelayToCarrierMap.size() > TopNumber) {
				minutesDelayToCarrierMap
						.remove(minutesDelayToCarrierMap.last());
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			for (Pair<Float, String> item : minutesDelayToCarrierMap) {
				String[] strings = { item.second, item.first.toString() };
				TextArrayWritable val = new TextArrayWritable(strings);
				context.write(NullWritable.get(), val);
			}
		}
	}

	public static class TopMinDelayReduce extends
			Reducer<NullWritable, TextArrayWritable, Text, FloatWritable> {

		private TreeSet<Pair<Float, String>> minutesDelayToCarrierMap = new TreeSet<Pair<Float, String>>();

		@Override
		public void reduce(NullWritable key,
				Iterable<TextArrayWritable> values, Context context)
				throws IOException, InterruptedException {
			for (TextArrayWritable val : values) {
				Text[] pair = (Text[]) val.toArray();
				String airline = pair[0].toString();
				Float minutesDelay = Float.parseFloat(pair[1].toString());
				minutesDelayToCarrierMap.add(new Pair<Float, String>(
						minutesDelay, airline));
				if (minutesDelayToCarrierMap.size() > TopNumber) {
					minutesDelayToCarrierMap.remove(minutesDelayToCarrierMap
							.last());
				}
			}
			for (Pair<Float, String> item : minutesDelayToCarrierMap) {
				Text word = new Text(item.second);
				FloatWritable value = new FloatWritable(item.first);
				context.write(word, value);
			}
		}
	}

	private static class FloatArrayWritable extends ArrayWritable {
		public FloatArrayWritable() {
			super(FloatArrayWritable.class);
		}

		public FloatArrayWritable(Float[] numbers) {
			super(FloatWritable.class);
			FloatWritable[] floats = new FloatWritable[numbers.length];
			for (int i = 0; i < numbers.length; i++) {
				floats[i] = new FloatWritable(numbers[i]);
			}
			set(floats);
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
