import java.io.IOException;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.collections.KeyValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;

import scala.Serializable;
import scala.Tuple2;

public class AiportDepPerfomanceToAirport implements IRunnableTask,
		Serializable {

	public void Run(String inputPath, Boolean runLocal) {

		String outputPath = "/user/capstone/output/Task2/Task2_2-2_AiportDepPerfomanceToAirport_output";

		FileSystem fs;
		try {
			fs = FileSystem.get(new Configuration());
			fs.delete(new Path(outputPath), true);
		} catch (IOException e) {
			e.printStackTrace();
		}

		SparkConf sparkConf = new SparkConf().setAppName(
				"Airport to airport departure perfomance").set(
				"spark.driver.maxResultSize", "3g");

		if (runLocal) {
			sparkConf.setMaster("local[4]");
		}
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<String> data = sc.textFile(inputPath).cache();

		JavaPairRDD<Tuple2<String, String>, Float> airportToAirportDelay = data
				.filter(new Function<String, Boolean>() {
					public Boolean call(String s) throws Exception {
						String[] splittedValues = s.split("\\|");
						if (splittedValues.length < 8) {
							return false;
						}
						String origin = splittedValues[Settings.ORIGIN_POSITION];
						String destination = splittedValues[Settings.DESTINATION_POSITION];
						String departureDelayString = splittedValues[Settings.DEPARTURE_DELAY_POSITION];
						if (origin.isEmpty() || destination.isEmpty()
								|| departureDelayString.isEmpty()) {
							return false;
						}

						return true;
					}
				})
				.mapToPair(
						new PairFunction<String, Tuple2<String, String>, Float>() {
							public Tuple2<Tuple2<String, String>, Float> call(
									String s) throws Exception {

								String[] splittedValues = s.split("\\|");
								String origin = splittedValues[Settings.ORIGIN_POSITION];
								String destination = splittedValues[Settings.DESTINATION_POSITION];
								String departureDelayString = splittedValues[Settings.DEPARTURE_DELAY_POSITION];

								Float departureDelay = Float
										.parseFloat(departureDelayString);
								Tuple2<String, String> originDestination = new Tuple2<String, String>(
										origin, destination);

								return new Tuple2<Tuple2<String, String>, Float>(
										originDestination, departureDelay);
							}
						});

		JavaPairRDD<Tuple2<String, String>, Integer> airportToAirportFlightCount = airportToAirportDelay
				.groupByKey()
				.mapToPair(
						new PairFunction<Tuple2<Tuple2<String, String>, Iterable<Float>>, Tuple2<String, String>, Integer>() {
							public Tuple2<Tuple2<String, String>, Integer> call(
									Tuple2<Tuple2<String, String>, Iterable<Float>> t)
									throws Exception {
								return new Tuple2<Tuple2<String, String>, Integer>(
										t._1(), ((Collection<?>) t._2).size());
							}
						});

		JavaPairRDD<Tuple2<String, String>, Float> airportToAirportSumDelay = airportToAirportDelay
				.reduceByKey(new Function2<Float, Float, Float>() {
					public Float call(Float v1, Float v2) throws Exception {
						return v1 + v2;
					}
				});

		JavaPairRDD<Tuple2<String, String>, Double> airportToAirportAverageDelay = airportToAirportSumDelay
				.join(airportToAirportFlightCount).mapValues(
						new Function<Tuple2<Float, Integer>, Double>() {
							public Double call(Tuple2<Float, Integer> tuple)
									throws Exception {
								Double averageValue = Math.round(tuple._1()
										/ tuple._2() * 100.0) / 100.0;
								return averageValue;
							}
						});

		JavaPairRDD<String, TreeSet<Pair<Double, String>>> airportsTopFlightsDelay = airportToAirportAverageDelay
				.mapToPair(
						new PairFunction<Tuple2<Tuple2<String, String>, Double>, String, Tuple2<Double, String>>() {
							public Tuple2<String, Tuple2<Double, String>> call(
									Tuple2<Tuple2<String, String>, Double> tuple)
									throws Exception {
								return new Tuple2<String, Tuple2<Double, String>>(
										tuple._1._1,
										new Tuple2<Double, String>(tuple._2,
												tuple._1._2));
							}
						})
				.groupByKey()
				.mapValues(
						new Function<Iterable<Tuple2<Double, String>>, TreeSet<Pair<Double, String>>>() {

							public TreeSet<Pair<Double, String>> call(
									Iterable<Tuple2<Double, String>> tupleList)
									throws Exception {

								TreeSet<Pair<Double, String>> delayToDestinationMap = new TreeSet<Pair<Double, String>>();

								for (Tuple2<Double, String> tuple : tupleList) {
									delayToDestinationMap
											.add(new Pair<Double, String>(
													tuple._1, tuple._2));
									if (delayToDestinationMap.size() > 10) {
										delayToDestinationMap
												.remove(delayToDestinationMap
														.last());
									}
								}

								return delayToDestinationMap;
							}
						});

		airportsTopFlightsDelay.saveAsTextFile(outputPath);

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
		final Table table = dynamoDB.getTable("2-2");

		List<Tuple2<String, TreeSet<Pair<Double, String>>>> listData = airportsTopFlightsDelay
				.cache().collect();

		for (Tuple2<String, TreeSet<Pair<Double, String>>> listItem : listData) {
			String key = listItem._1;
			String value = listItem._2.toString();
			table.putItem(new Item().withPrimaryKey("origin", key).with("airlines",
					value));
		}
	}
}
