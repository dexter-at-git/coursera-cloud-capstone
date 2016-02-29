import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;

import scala.Array;
import scala.Serializable;
import scala.Tuple2;
import scala.collection.SortedMap;

public class OnTimeAirlines implements IRunnableTask, Serializable {

	public void Run(String inputPath, Boolean runLocal) {

		String outputPath = "/user/capstone/output/Task2/Task2_1-2_OnTimeAirlines_output";
		;

		FileSystem fs;
		try {
			fs = FileSystem.get(new Configuration());
			fs.delete(new Path(outputPath), true);
		} catch (IOException e) {
			e.printStackTrace();
		}

		SparkConf sparkConf = new SparkConf().setAppName("On Time Airlines")
				.set("spark.driver.maxResultSize", "3g");

		if (runLocal) {
			sparkConf.setMaster("local[4]");
		}

		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<String> data = sc.textFile(inputPath).cache();

		JavaPairRDD<String, Double> airlaineDalay = data.filter(
				new Function<String, Boolean>() {
					public Boolean call(String s) throws Exception {
						String[] splittedValues = s.split("\\|");
						if (splittedValues.length < 9) {
							return false;
						}
						String airline = splittedValues[Settings.CARRIER_POSITION];
						String arrivalDelayString = splittedValues[Settings.ARRIVAL_DELAY_POSITION];
						if (airline.isEmpty() || arrivalDelayString.isEmpty()) {
							return false;
						}
						return true;
					}
				}).mapToPair(new PairFunction<String, String, Double>() {
			public Tuple2<String, Double> call(String s) throws Exception {
				String[] splittedValues = s.split("\\|");

				String airline = splittedValues[Settings.CARRIER_POSITION];
				String arrivalDelayString = splittedValues[Settings.ARRIVAL_DELAY_POSITION];

				Double arrivalDelay = Math.round(Double
						.parseDouble(arrivalDelayString) * 100.0) / 100.0;

				ArrayList<Tuple2<String, Double>> airlines = new ArrayList<Tuple2<String, Double>>();
				airlines.add(new Tuple2<String, Double>(airline, arrivalDelay));

				return new Tuple2<String, Double>(airline, arrivalDelay);
			}
		});

		JavaPairRDD<String, Integer> airlineDelayCount = airlaineDalay
				.groupByKey()
				.mapToPair(
						new PairFunction<Tuple2<String, Iterable<Double>>, String, Integer>() {

							public Tuple2<String, Integer> call(
									Tuple2<String, Iterable<Double>> tuple)
									throws Exception {
								return new Tuple2<String, Integer>(tuple._1,
										((Collection<?>) tuple._2).size());
							}
						});

		JavaPairRDD<String, Double> airlineDelaySum = airlaineDalay
				.reduceByKey(new Function2<Double, Double, Double>() {
					public Double call(Double v1, Double v2) throws Exception {
						return v1 + v2;
					}
				});

		JavaPairRDD<String, Double> airlineAverageFlights = airlineDelaySum
				.join(airlineDelayCount).mapValues(
						new Function<Tuple2<Double, Integer>, Double>() {
							public Double call(Tuple2<Double, Integer> tuple)
									throws Exception {
								return tuple._1 / tuple._2;
							}
						});

		JavaPairRDD<String, Double> sortedAirlines = airlineAverageFlights
				.mapToPair(
						new PairFunction<Tuple2<String, Double>, Double, String>() {

							public Tuple2<Double, String> call(
									Tuple2<String, Double> t) throws Exception {
								return t.swap();
							}
						})
				.sortByKey()
				.mapToPair(
						new PairFunction<Tuple2<Double, String>, String, Double>() {

							public Tuple2<String, Double> call(
									Tuple2<Double, String> t) throws Exception {
								return t.swap();
							}
						});

		JavaPairRDD<String, Double> top10 = sc.parallelizePairs(sortedAirlines
				.take(10));

		top10.saveAsTextFile(outputPath);

		AmazonDynamoDBClient client = new AmazonDynamoDBClient(
				new AWSCredentials() {
					public String getAWSSecretKey() {
						return Settings.SecretAccessKey;
					}

					public String getAWSAccessKeyId() {
						return Settings.AccessKeyID;
					}
				});

		DynamoDB dynamoDB = new DynamoDB(client);
		final Table table = dynamoDB.getTable("1-2");

		List<Tuple2<String, Double>> listData = top10.cache().collect();

		for (Tuple2<String, Double> listItem : listData) {
			String key = listItem._1;
			Double value = listItem._2;
			table.putItem(new Item().withPrimaryKey("airline", key).withDouble(
					"delay", value));
		}
	}
}