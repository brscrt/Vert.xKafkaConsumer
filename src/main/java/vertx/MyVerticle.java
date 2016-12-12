package vertx;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import util.ConfigConstants;
import util.KafkaEvent;
import util.MyProperties;

public class MyVerticle extends AbstractVerticle {

	private static final Logger logger = LoggerFactory.getLogger(MyVerticle.class);
	private EventBus bus;
	private AtomicBoolean running;
	private JsonObject verticleConfig;
	private String busAddress;

	private KafkaConsumer<String, String> consumer;
	private ExecutorService backgroundConsumer;

	private List<String> topics;

	// Called when verticle is deployed
	public void start(final Future<Void> startedResult) {
		try {
			bus = vertx.eventBus();
			running = new AtomicBoolean(true);
			verticleConfig = config();
			MyProperties myProperties = new MyProperties();
			myProperties.loadProperties("conf/myconsumer.properties");
			consumer = new KafkaConsumer<>(myProperties);
			JsonArray topicConfig = verticleConfig.getJsonArray("topics");

			busAddress = "kafka.message.consumer";

			Runtime.getRuntime().addShutdownHook(new Thread() {
				// try to disconnect from ZK as gracefully as possible
				public void run() {
					shutdown();
				}
			});

			backgroundConsumer = Executors.newSingleThreadExecutor();
			backgroundConsumer.submit(() -> {
				try {

					topics = new ArrayList<>();
					for (int i = 0; i < topicConfig.size(); i++) {
						topics.add(topicConfig.getString(i));
						logger.info("Subscribing to topic ");
					}

					// signal success before we enter read loop
					startedResult.complete();
					consume();
				} catch (Exception ex) {
					String error = "Failed to startup";
					logger.error(error, ex);
					bus.send(ConfigConstants.CONSUMER_ERROR_TOPIC, getErrorString(error, ex.getMessage()));
					startedResult.fail(ex);
				}
			});

		} catch (Exception ex) {
			String error = "Failed to startup";
			logger.error(error, ex);
			bus.send(ConfigConstants.CONSUMER_ERROR_TOPIC, getErrorString("Failed to startup", ex.getMessage()));
			startedResult.fail(ex);
		}
	}

	private String getErrorString(String error, String errorMessage) {
		return String.format("%s - error: %s", error, errorMessage);
	}

	// Optional - called when verticle is undeployed
	public void stop() {
	}

	/**
	 * Handles looping and consuming
	 */
	private void consume() {
		consumer.subscribe(topics);
		while (running.get()) {
			try {
				ConsumerRecords<String, String> records = consumer.poll(100);

				// there were no messages
				if (records == null) {
					continue;
				}

				Iterator<ConsumerRecord<String, String>> iterator = records.iterator();

				// roll through and put each kafka message on the event bus
				while (iterator.hasNext()) {
					sendMessage(iterator.next());
				}

			} catch (Exception ex) {
				String error = "Error consuming messages from kafka";
				logger.error(error, ex);
				bus.send(ConfigConstants.CONSUMER_ERROR_TOPIC, getErrorString(error, ex.getMessage()));
			}
		}
	}

	private void sendMessage(ConsumerRecord<String, String> record) {
		try {
			bus.send(busAddress, KafkaEvent.createEventForBus(record));
		} catch (Exception ex) {
			String error = String.format("Error sending messages on event bus - record: %s", record.toString());
			logger.error(error, ex);
			bus.send(ConfigConstants.CONSUMER_ERROR_TOPIC, getErrorString(error, ex.getMessage()));
		}
	}

	private void shutdown() {
		running.compareAndSet(true, false);
		try {
			if (consumer != null) {
				try {
					consumer.unsubscribe();
					consumer.close();
					consumer = null;
				} catch (Exception ex) {
				}
			}

			if (backgroundConsumer != null) {
				backgroundConsumer.shutdown();
				backgroundConsumer = null;
			}
		} catch (Exception ex) {
			logger.error("Failed to close consumer", ex);
		}
	}
}
