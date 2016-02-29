import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;

import scala.Serializable;
import scala.Tuple2;
import scala.Tuple3;

public class TwoLegs implements IRunnableTask, Serializable {

	public void Run(String inputPath, Boolean runLocal) {

		String outputPath = "/user/capstone/output/Task2/Task2_3-2_TwoLegs_output";
		inputPath = "/user/capstone/pigoutput/15_02-22_00_legs/";
	//	inputPath = "D://part-r-00010";

		FileSystem fs;
		try {
			fs = FileSystem.get(new Configuration());
			fs.delete(new Path(outputPath), true);
		} catch (IOException e) {
			e.printStackTrace();
		}

		SparkConf sparkConf = new SparkConf().setAppName("Two legs").set(
				"spark.driver.maxResultSize", "3g");

		if (runLocal) {
			sparkConf.setMaster("local[4]");
		}
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<String> data = sc.textFile(inputPath).cache();

		JavaPairRDD<Tuple3<String, String, String>, Float> allDelays = data
				.filter(new Function<String, Boolean>() {
					public Boolean call(String s) throws Exception {
						String[] splittedValues = s.split("\\|");
						if (splittedValues.length < 9) {
							return false;
						}
						String origin = splittedValues[Settings.ORIGIN_POSITION];
						String destination = splittedValues[Settings.DESTINATION_POSITION];
						String dateString = splittedValues[Settings.FLIGHT_DATE_POSITION];
						String arrivalDelayString = splittedValues[Settings.ARRIVAL_DELAY_POSITION];
						String departureTimeString = splittedValues[Settings.DEPARTURE_TIME_POSITION];
						if (origin.isEmpty() || destination.isEmpty()
								|| arrivalDelayString.isEmpty()
								|| dateString.isEmpty()
								|| departureTimeString.isEmpty()) {
							return false;
						}
						
						origin = origin.replace("\"", "");
						destination = destination.replace("\"", "");
						dateString = dateString.replace("\"", "");
										
						//CMI ORD LAX, 04/03/2008:
						if ((origin.equals("CMI") && (destination.equals("ORD")) && dateString.equals("2008-03-04")) 
						 || (origin.equals("ORD") && (destination.equals("LAX")) && dateString.equals("2008-03-06"))) {
							return true;
						}
						
						//JAX DFW CRP, 09/09/2008:
						if ((origin.equals("JAX") && (destination.equals("DFW")) && dateString.equals("2008-09-09")) 
						 || (origin.equals("DFW") && (destination.equals("CRP")) && dateString.equals("2008-09-11"))) {
							return true;
						}
						
						//SLC BFL LAX, 01/04/2008:
						if ((origin.equals("SLC") && (destination.equals("BFL")) && dateString.equals("2008-04-01")) 
						 || (origin.equals("BFL") && (destination.equals("LAX")) && dateString.equals("2008-04-03"))) {
							return true;
						}
						
						//DFW ORD DFW, 10/06/2008:	
						if ((origin.equals("DFW") && (destination.equals("ORD")) && dateString.equals("2008-06-10")) 
						 || (origin.equals("ORD") && (destination.equals("DFW")) && dateString.equals("2008-06-12"))) {
							return true;
						}
						


						/////////////////////
						//CMI ORD LAX, 04/03/2008:
						if ((origin.equals("BOS") && (destination.equals("ATL")) && dateString.equals("2008-04-03")) 
						 || (origin.equals("ATL") && (destination.equals("LAX")) && dateString.equals("2008-04-05"))) {
							return true;
						}
						
						//JAX DFW CRP, 09/09/2008:
						if ((origin.equals("PHX") && (destination.equals("JFK")) && dateString.equals("2008-09-07")) 
						 || (origin.equals("JFK") && (destination.equals("MSP")) && dateString.equals("2008-09-09"))) {
							return true;
						}
						
						//SLC BFL LAX, 01/04/2008:
						if ((origin.equals("DFW") && (destination.equals("STL")) && dateString.equals("2008-01-24")) 
						 || (origin.equals("STL") && (destination.equals("ORD")) && dateString.equals("2008-01-26"))) {
							return true;
						}
						
						//DFW ORD DFW, 10/06/2008:	
						if ((origin.equals("LAX") && (destination.equals("MIA")) && dateString.equals("2008-05-16")) 
						 || (origin.equals("MIA") && (destination.equals("LAX")) && dateString.equals("2008-05-18"))) {
							return true;
						}
						
						return false;
					}
				})
				.mapToPair(
						new PairFunction<String, Tuple3<String, String, String>, Float>() {
							public Tuple2<Tuple3<String, String, String>, Float> call(
									String s) throws Exception {

								String[] splittedValues = s.split("\\|");

								String origin = splittedValues[Settings.ORIGIN_POSITION];
								String destination = splittedValues[Settings.DESTINATION_POSITION];
								String airline = splittedValues[Settings.CARRIER_POSITION];
								String flight = splittedValues[Settings.FLIGHT_POSITION];
								String dateString = splittedValues[Settings.FLIGHT_DATE_POSITION];
								String arrivalDelayString = splittedValues[Settings.ARRIVAL_DELAY_POSITION];
								String departureTimeString = splittedValues[Settings.DEPARTURE_TIME_POSITION];

								Float arrivalDelay = Float
										.parseFloat(arrivalDelayString);
								DateFormat df = new SimpleDateFormat(
										"yyyy-MM-dd");
								Date date = df.parse(dateString);
								airline = airline + "-" + flight;
								airline = airline.replace("\"", "");
								dateString = dateString + "/" + departureTimeString + "/"
										+ airline;

								Tuple3<String, String, String> originDestination = new Tuple3<String, String, String>(
										origin, destination, dateString);

								return new Tuple2<Tuple3<String, String, String>, Float>(
										originDestination, arrivalDelay);
							}
						});

		JavaPairRDD<Tuple2<String, String>, Tuple2<String, Double>> minDelays = allDelays
				.groupByKey()
				.mapToPair(
						new PairFunction<Tuple2<Tuple3<String, String, String>, Iterable<Float>>, Tuple2<String, String>, Tuple2<String, Double>>() {
							public Tuple2<Tuple2<String, String>, Tuple2<String, Double>> call(
									Tuple2<Tuple3<String, String, String>, Iterable<Float>> t)
									throws Exception {
								Collection<Float> collection = (Collection<Float>) t._2;
								Double minValue = Math.round(Collections
										.min(collection) * 100.0) / 100.0;

								return new Tuple2<Tuple2<String, String>, Tuple2<String, Double>>(
										new Tuple2<String, String>(t._1()._1(),
												t._1()._2()),
										new Tuple2<String, Double>(t._1()._3(),
												minValue));
							}
						});

		// JavaPairRDD<String, Tuple3<String, Date, Double>> firstLeg =
		// minDelays
		// .mapToPair(new PairFunction<Tuple2<Tuple2<String, String>,
		// Tuple2<Date, Double>>, String, Tuple3<String, Date, Double>>() {
		// public Tuple2<String, Tuple3<String, Date, Double>> call(
		// Tuple2<Tuple2<String, String>, Tuple2<Date, Double>> t)
		// throws Exception {
		// String destination = t._1()._2();
		// Tuple3<String, Date, Double> tuple = new Tuple3<String, Date,
		// Double>(
		// t._1()._1(), t._2()._1(), t._2()._2());
		// return new Tuple2<String, Tuple3<String, Date, Double>>(
		// destination, tuple);
		// }
		// });
		//
		// JavaPairRDD<String, Tuple3<String, Date, Double>> secondLeg =
		// minDelays
		// .mapToPair(new PairFunction<Tuple2<Tuple2<String, String>,
		// Tuple2<Date, Double>>, String, Tuple3<String, Date, Double>>() {
		// public Tuple2<String, Tuple3<String, Date, Double>> call(
		// Tuple2<Tuple2<String, String>, Tuple2<Date, Double>> t)
		// throws Exception {
		// String origin = t._1()._1();
		// Tuple3<String, Date, Double> tuple = new Tuple3<String, Date,
		// Double>(
		// t._1()._2(), t._2()._1(), t._2()._2());
		// return new Tuple2<String, Tuple3<String, Date, Double>>(
		// origin, tuple);
		// }
		// });
		//
		// JavaPairRDD<String, Tuple2<Tuple3<String, Date, Double>,
		// Tuple3<String, Date, Double>>> q = firstLeg.join(secondLeg);
		//
		/*
		 * JavaPairRDD<Tuple2<String, String>, Float> airportToAirportSumDelay =
		 * airportToAirportDelay .reduceByKey(new Function2<Float, Float,
		 * Float>() { public Float call(Float v1, Float v2) throws Exception {
		 * return v1 + v2; } });
		 * 
		 * JavaPairRDD<Tuple2<String, String>, Double>
		 * airportToAirportAverageDelay = airportToAirportSumDelay
		 * .join(airportToAirportFlightCount).mapValues( new
		 * Function<Tuple2<Float, Integer>, Double>() { public Double
		 * call(Tuple2<Float, Integer> tuple) throws Exception { Double
		 * averageValue = Math.round(tuple._1() / tuple._2() * 100.0) / 100.0;
		 * return averageValue; } });
		 */
		minDelays.saveAsTextFile(outputPath);

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
		final Table table = dynamoDB.getTable("3-2");

		List<Tuple2<Tuple2<String, String>, Tuple2<String, Double>>> listData = minDelays
				.cache().collect();

		for (Tuple2<Tuple2<String, String>, Tuple2<String, Double>> listItem : listData) {
			String origin = listItem._1()._1();
			String destination = listItem._1()._2();
			Double delay = listItem._2()._2();
			String airline = listItem._2()._1().split("/")[2];
			String date = listItem._2()._1().split("/")[0];
			String time = listItem._2()._1().split("/")[1];

			String key = origin + "/" + destination + "/" + date + "/" + time;
			key = key.replace("\"", "");
			table.putItem(new Item().withPrimaryKey("origin", key).with("airline", airline)
					.with("delay", delay));
		}
	}
}
