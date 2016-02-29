import java.util.Date;

public class Main {

	public static void main(String[] args) {

		String inputPath;
		IRunnableTask task;

		if (args.length == 0) {
			task = new TwoLegs();
			inputPath = "D:\\part-r-00009";
			task.Run(inputPath, true);
			return;
		}

		String argConfig = args[0];
		if (argConfig.equals("1-1")) {
			System.out.println("Run configuration - MostPopularAirports");
			task = new MostPopularAirports();
		} else if (argConfig.equals("1-2")) {
			System.out.println("Run configuration - OnTimeAirlines");
			task = new OnTimeAirlines();
		} else if (argConfig.equals("2-1")) {
			System.out
					.println("Run configuration - AiportDepPerfomanceCarrier");
			task = new AiportDepPerfomanceCarrier();
		} else if (argConfig.equals("2-2")) {
			System.out
					.println("Run configuration - AiportDepPerfomanceToAirport");
			task = new AiportDepPerfomanceToAirport();
		} else if (argConfig.equals("2-4")) {
			System.out.println("Run configuration - MeanArrivalDelay");
			task = new MeanArrivalDelay();
		} else if (argConfig.equals("3-1")) {
			System.out.println("Run configuration - AllAirports");
			task = new AllAirports();
		} else if (argConfig.equals("3-2")) {
			System.out.println("Run configuration - TwoLegs");
			task = new TwoLegs();
		} else if (argConfig.equals("stream")) {
			System.out.println("Run configuration - StreamingExamlpe");
			task = new StreamingExamlpe();
		} else {
			System.out.println("Invalid run configuration");
			return;
		}

		inputPath = "/user/capstone/pigoutput/14_02-18_00/";

		// inputPath = "/user/capstone/rawaviadata/";
		task.Run(inputPath, false);
	}

}
