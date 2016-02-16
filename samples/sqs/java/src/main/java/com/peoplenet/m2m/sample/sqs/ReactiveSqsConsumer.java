package com.peoplenet.m2m.sample.sqs;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.sqs.AmazonSQSAsyncClient;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.peoplenet.m2m.sample.sqs.metrics.Metrics;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscriber;
import rx.schedulers.Schedulers;

public class ReactiveSqsConsumer implements Runnable {

	private static final Logger logger = LoggerFactory.getLogger(ReactiveSqsConsumer.class);

	private final String queueUrl;

	private final AmazonSQSAsyncClient sqs;

	private final MessageHandler messageHandler;

	private final Metrics metrics = new Metrics();

	public ReactiveSqsConsumer(String queueUrl) {

		this.queueUrl = queueUrl;
		this.sqs = new AmazonSQSAsyncClient(new DefaultAWSCredentialsProviderChain().getCredentials());
		this.messageHandler = new MessageHandler();
	}

	public void run() {

		logger.info("Starting ReactiveSqsConsumer");
		Observable
				.create(onSubscribe())
				.observeOn(Schedulers.io())
				.flatMap(this::onReceiveMessages)
				.subscribeOn(Schedulers.io())
				.subscribe(this::onMessageHandlerResults);
	}

	private Observable.OnSubscribe<ReceiveMessageResult> onSubscribe() {
		return (Subscriber<? super ReceiveMessageResult> subscriber) -> {
			while (true) {
				try {
					receiveMessages(subscriber);
				} catch (QueueDoesNotExistException e) {
					logger.error("Queue does not exist for queueUrl:" + queueUrl, e);
					subscriber.onError(e);
					throw e; // kill the thread
				} catch (Throwable t) {
					logger.error("Error attempting to consume from queueUrl:" + queueUrl, t);
					logger.info("Waiting 5 seconds before retry.");
					sleep(5000);
				}
			}
		};
	}

	private void receiveMessages(Subscriber<? super ReceiveMessageResult> subscriber) throws Throwable {
		try {
			sqs.receiveMessageAsync(
					new ReceiveMessageRequest()
							.withMaxNumberOfMessages(10)
							.withQueueUrl(queueUrl),
					new ReceiveMessageAsyncHandler(subscriber)
			).get();
		} catch (ExecutionException e) {
			throw e.getCause();
		}
	}

	private Observable<List<MessageHandlerResult>> onReceiveMessages(ReceiveMessageResult receiveMessageResult) {
		return Observable.just(receiveMessageResult.getMessages().stream()
				.map(message -> new MessageHandlerResult(message.getReceiptHandle(), messageHandler.handleMessage(message)))
				.filter(MessageHandlerResult::isSuccess)
				.collect(Collectors.toList()));
	}

	private void onMessageHandlerResults(List<MessageHandlerResult> messageHandlerResults) {
		sqs.deleteMessageBatch(new DeleteMessageBatchRequest()
				.withQueueUrl(queueUrl)
				.withEntries(IntStream.range(0, messageHandlerResults.size())
						.mapToObj(i -> new DeleteMessageBatchRequestEntry(Integer.toString(i), messageHandlerResults.get(i).receiptHandle))
						.collect(Collectors.toList())));
	}

	private class ReceiveMessageAsyncHandler implements AsyncHandler<ReceiveMessageRequest, ReceiveMessageResult> {

		private final Subscriber<? super ReceiveMessageResult> subscriber;

		public ReceiveMessageAsyncHandler(Subscriber<? super ReceiveMessageResult> subscriber) {
			this.subscriber = subscriber;
		}

		@Override public void onSuccess(ReceiveMessageRequest request, ReceiveMessageResult receiveMessageResult) {
			if (!subscriber.isUnsubscribed()) {
				subscriber.onNext(receiveMessageResult);
			}
		}

		@Override public void onError(Exception exception) {
			logger.error("Exception in async handler");
		}
	}

	public class MessageHandler {

		public boolean handleMessage(Message message) {
			// add processing logic here
			metrics.received();
			return true;
		}
	}

	private class MessageHandlerResult {

		private final String receiptHandle;
		private final boolean success;

		public MessageHandlerResult(String receiptHandle, boolean success) {
			this.receiptHandle = receiptHandle;
			this.success = success;
		}

		public String getReceiptHandle() {
			return receiptHandle;
		}

		public boolean isSuccess() {
			return success;
		}

		@Override public String toString() {
			return "MessageHandlerResult{" +
					"receiptHandle='" + receiptHandle + '\'' +
					", success=" + success +
					'}';
		}
	}

	private void sleep(long millis) {

		try {
			Thread.sleep(millis);
		} catch (Exception e) {
			logger.error("Exception in sleep", e);
		}
	}

}
