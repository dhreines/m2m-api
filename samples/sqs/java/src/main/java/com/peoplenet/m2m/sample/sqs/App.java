package com.peoplenet.m2m.sample.sqs;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

public class App {

	private static final int senderThreads = 10;
	private static final int consumerThreads = 10;
	private static final ExecutorService consumerExecutor = Executors.newFixedThreadPool(consumerThreads);
	private static final ExecutorService senderExecutor = Executors.newFixedThreadPool(senderThreads);

	/**
	 * Consumes and logs messages from the specified queue.
	 *
	 * @param args args[0] consumption mode - either 'sync', 'async' or 'rx'
	 * @param args args[1] the sqs queue url
	 */
	public static void main(String[] args) {

		if (args.length != 2) {
			System.out.println("Please specify the consumption mode (sync, async or rx) followed by the SQS queue url.");
			System.exit(1);
		}
		String consumerMode = args[0];
		String queueUrl = args[1];

		// start the consumer
		final Runnable consumer = createConsumer(consumerMode, queueUrl);
		IntStream.range(0, consumerThreads).forEach((i) -> consumerExecutor.submit(consumer));

		// Start the sender
		SimpleSqsSender sender = new SimpleSqsSender(queueUrl);
		IntStream.range(0, senderThreads).forEach((i) -> senderExecutor.submit(sender));
	}

	private static Runnable createConsumer(String consumerMode, String queueUrl) {
		final Runnable consumer;
		switch (consumerMode) {
		case "sync":
			consumer = new SimpleSqsConsumer(queueUrl);
			break;
		case "async":
			consumer = new AsyncSqsConsumer(queueUrl);
			break;
		case "rx":
			consumer = new ReactiveSqsConsumer(queueUrl);
			break;
		default:
			System.out.println("Please specify the consumption mode as either 'sync', 'async' or 'rx'");
			throw new IllegalArgumentException();
		}
		return consumer;
	}

}
