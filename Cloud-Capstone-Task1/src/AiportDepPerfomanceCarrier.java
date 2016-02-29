import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Date;
import java.util.TreeSet;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
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

public class AiportDepPerfomanceCarrier extends Configured implements Tool {

	private static int CARRIER_POSITION = 2;
	private static int ORIGIN_POSITION = 3;
	private static int DESTINATION_POSITION = 4;
	private static int DEPARTURE_DELAY_MINUTES_POSITION = 7;
	private static int IS_DEPARTURE_DELAY_POSITION = 6;
	private static int ARRIVAL_DELAY_MINUTES_POSITION = 7;
	private static int IS_ARRIVAL_DELAY_POSITION = 8;
	private static int AIRLINE_ID_POSITION = 9;
	private static int TopNumber = 10;

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new AiportDepPerfomanceCarrier(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		FileSystem fs = FileSystem.get(conf);
		Path tmpPath = new Path("/user/capstone/tmp");
		Path outputPath = new Path(args[1]);
		fs.delete(tmpPath, true);
		fs.delete(outputPath, true);

		Job jobA = Job.getInstance(conf, "Carrier");
		jobA.setOutputKeyClass(TextPair.class);
		jobA.setOutputValueClass(FloatWritable.class);

		jobA.setMapperClass(OriginDestPerfomanceMap.class);
		jobA.setReducerClass(OriginDestPerfomanceReduce.class);

		FileInputFormat.setInputPaths(jobA, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobA, tmpPath);

		jobA.setJarByClass(AiportDepPerfomanceCarrier.class);
		jobA.waitForCompletion(true);

		Job jobB = Job.getInstance(conf, "Departure delay");
		jobB.setOutputKeyClass(Text.class);
		jobB.setOutputValueClass(FloatWritable.class);

		jobB.setMapOutputKeyClass(Text.class);
		jobB.setMapOutputValueClass(TextArrayWritable.class);

		jobB.setMapperClass(CarrierDelayMap.class);
		jobB.setReducerClass(CarrierDelayReduce.class);
		jobB.setNumReduceTasks(1);

		FileInputFormat.setInputPaths(jobB, tmpPath);
		FileOutputFormat.setOutputPath(jobB, outputPath);

		jobB.setInputFormatClass(KeyValueTextInputFormat.class);
		jobB.setOutputFormatClass(TextOutputFormat.class);

		jobB.setJarByClass(OnTimeAirlines.class);
		return jobB.waitForCompletion(true) ? 0 : 1;

	}

	public static class OriginDestPerfomanceMap extends
			Mapper<Object, Text, TextPair, FloatWritable> {

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
			String carrier = values[CARRIER_POSITION];
			String departureDelayString = values[DEPARTURE_DELAY_MINUTES_POSITION];
			if (originAirport.isEmpty() || carrier.isEmpty()
					|| departureDelayString.isEmpty()) {
				return;
			}
			Float departureDelay = Float.parseFloat(departureDelayString);
			TextPair originDestinationPair = new TextPair(new Text(
					originAirport), new Text(carrier));

			context.write(originDestinationPair, new FloatWritable(
					departureDelay));
		}
	}

	public static class OriginDestPerfomanceReduce extends
			Reducer<TextPair, FloatWritable, Text, Text> {

		private IntWritable result = new IntWritable();

		@Override
		public void reduce(TextPair key, Iterable<FloatWritable> values,
				Context context) throws IOException, InterruptedException {
			float delay = 0;
			int count = 0;
			for (FloatWritable val : values) {
				delay += val.get();
				count++;
			}
			Float avgDelay = delay / count;
			context.write(new Text(key.first.toString()), new Text(key.second
					+ "-" + avgDelay.toString()));
		}
	}

	public static class CarrierDelayMap extends
			Mapper<Text, Text, Text, TextArrayWritable> {

		@Override
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			String origin = key.toString();
			String[] values = value.toString().split("-");
			if (values.length != 2) {
				return;
			}
			String carrier = values[0].toString();
			Float departureDelay = Float.parseFloat(values[1]);

			TreeSet<Pair<Float, String>> delayToCarrierMap = new TreeSet<Pair<Float, String>>();
			delayToCarrierMap.add(new Pair<Float, String>(departureDelay,
					carrier));
			if (delayToCarrierMap.size() > TopNumber) {
				delayToCarrierMap.remove(delayToCarrierMap.last());
			}

			for (Pair<Float, String> item : delayToCarrierMap) {
				String[] strings = { item.second, item.first.toString() };
				TextArrayWritable val = new TextArrayWritable(strings);
				context.write(new Text(origin), val);
			}
		}
	}

	public static class CarrierDelayReduce extends
			Reducer<Text, TextArrayWritable, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<TextArrayWritable> values,
				Context context) throws IOException, InterruptedException {
			TreeSet<Pair<Float, String>> delayToCarrierMap = new TreeSet<Pair<Float, String>>();
			for (TextArrayWritable val : values) {
				Text[] destinationDelayPair = (Text[]) val.toArray();
				String carrier = destinationDelayPair[0].toString();
				Float delay = Float.parseFloat(destinationDelayPair[1]
						.toString());
				delayToCarrierMap.add(new Pair<Float, String>(delay, carrier));
				if (delayToCarrierMap.size() > TopNumber) {
					delayToCarrierMap.remove(delayToCarrierMap.last());
				}
			}

			StringBuilder destinationDelay = new StringBuilder();
			for (Pair<Float, String> item : delayToCarrierMap) {
				destinationDelay.append(item.second);
				destinationDelay.append("(");
				destinationDelay.append(new DecimalFormat("#.##").format(item.first));
				destinationDelay.append("min) ");
			}

			context.write(key, new Text(destinationDelay.toString()));
		}
	}

	private static class TextPair implements WritableComparable {

		Text first;
		Text second;

		public TextPair() {
		}

		public TextPair(Text t1, Text t2) {
			first = t1;
			second = t2;
		}

		public void setFirstText(String t1) {
			first.set(t1.getBytes());
		}

		public void setSecondText(String t2) {
			second.set(t2.getBytes());
		}

		public Text getFirst() {
			return first;
		}

		public Text getSecond() {
			return second;
		}

		public void write(DataOutput out) throws IOException {
			first.write(out);
			second.write(out);
		}

		public void readFields(DataInput in) throws IOException {
			if (first == null)
				first = new Text();

			if (second == null)
				second = new Text();

			first.readFields(in);
			second.readFields(in);
		}

		public int compareTo(Object object) {
			TextPair ip2 = (TextPair) object;
			int cmp = getFirst().compareTo(ip2.getFirst());
			if (cmp != 0) {
				return cmp;
			}
			return getSecond().compareTo(ip2.getSecond()); // reverse
		}

		public int hashCode() {
			return first.hashCode();
		}

		public boolean equals(Object o) {
			TextPair p = (TextPair) o;
			return first.equals(p.getFirst());
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
}
