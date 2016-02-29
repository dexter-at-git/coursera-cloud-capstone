import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

public class MostPopularAirports implements IRunnableTask, Serializable {

	public void Run(String inputPath, Boolean runLocal) {

		String outputPath = "/user/capstone/output/Task2/Task2_1-1_MostPopularAirports_output";

		FileSystem fs;
		try {
			fs = FileSystem.get(new Configuration());
			fs.delete(new Path(outputPath), true);
		} catch (IOException e) {
			e.printStackTrace();
		}

		SparkConf sparkConf = new SparkConf().setAppName(
				"Most Popular Airports")
				.set("spark.driver.maxResultSize", "3g");

		if (runLocal) {
			sparkConf.setMaster("local[4]");
		}

		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<String> data = sc.textFile(inputPath).cache();

		JavaRDD<String> airports = data
				.flatMap(new FlatMapFunction<String, String>() {
					public Iterable<String> call(String s) {
						String[] splittedValues = s.split("\\|");

						if (splittedValues.length < 9) {
							return new ArrayList<String>();
						}

						String origin = splittedValues[Settings.ORIGIN_POSITION];
						String destination = splittedValues[Settings.DESTINATION_POSITION];

						if (origin.isEmpty() || destination.isEmpty()) {
							return new ArrayList<String>();
						}
						ArrayList<String> airports = new ArrayList<String>();
						airports.add(origin);
						airports.add(destination);

						return airports;
					}
				});

		JavaPairRDD<String, Integer> pairs = airports
				.mapToPair(new PairFunction<String, String, Integer>() {
					public Tuple2<String, Integer> call(String s) {
						return new Tuple2<String, Integer>(s, 1);
					}
				});

		JavaPairRDD<String, Integer> counts = pairs
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					public Integer call(Integer a, Integer b) {
						return a + b;
					}
				});

		JavaPairRDD<String, Integer> sorted = counts
				.mapToPair(
						new PairFunction<Tuple2<String, Integer>, Integer, String>() {
							public Tuple2<Integer, String> call(
									Tuple2<String, Integer> t) throws Exception {
								return t.swap();
							}
						})
				.sortByKey(false)
				.mapToPair(
						new PairFunction<Tuple2<Integer, String>, String, Integer>() {
							public Tuple2<String, Integer> call(
									Tuple2<Integer, String> t) throws Exception {
								return t.swap();
							}
						});

		List<Tuple2<String, Integer>> top10 = sorted.take(10);

		JavaPairRDD<String, Integer> finalTop10 = sc.parallelizePairs(top10);

		finalTop10.saveAsTextFile(outputPath);

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
		final Table table = dynamoDB.getTable("1-1");

		List<Tuple2<String, Integer>> listData = finalTop10.cache().collect();

		for (Tuple2<String, Integer> listItem : listData) {
			String key = listItem._1;
			Integer value = listItem._2;
			table.putItem(new Item().withPrimaryKey("airport", key).withInt(
					"flights_number", value));
		}
	}
}