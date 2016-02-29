import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class StreamingExamlpe implements IRunnableTask {

	public void Run(String inputPath, Boolean runLocal) {

		SparkConf sparkConf = new SparkConf().setAppName(
				"Airport carrier departure perfomance").set(
				"spark.driver.maxResultSize", "3g");

		if (runLocal) {
			sparkConf.setMaster("local[4]");
		}

		JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf,
				Durations.seconds(2));
		
		JavaDStream<String> lines = streamingContext.textFileStream(inputPath);
	
		lines.print();
		
		streamingContext.start();
		streamingContext.awaitTermination(); 		
	}
}
