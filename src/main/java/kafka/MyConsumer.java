package kafka;

import java.util.ArrayList;
import java.util.List;

//import dene.consume.KafkaEvent;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import vertx.MyVerticle;

public class MyConsumer {

	static Vertx vertx;

	public static void main(String[] args) {
		consume();
		listen();
		writeToMongo();
	}

	private static void listen() {
		vertx.eventBus().consumer("kafka.message.consumer", message -> {
			System.out.println(String.format("got message: %s", message.body()));
			// message handling code
			// KafkaEvent event = new KafkaEvent((JsonObject) message.body());
		});
	}
	private static void writeToMongo(){
		vertx.eventBus().consumer("kafka.message.consumer", message -> {
			System.out.println(String.format("got message: %s", message.body()));
			// message handling code
			// KafkaEvent event = new KafkaEvent((JsonObject) message.body());
		});
	}

	private static void consume() {
		vertx = Vertx.vertx();

		// sample config
		JsonObject props = new JsonObject();
		List<String> topics = new ArrayList<>();
		topics.add("mytopic");
		topics.add("TEKTU");
		props.put("topics", new JsonArray(topics));

		deployKafka(props);

	}

	public static void deployKafka(JsonObject config) {
		// use your vert.x reference to deploy the consumer verticle

		vertx.deployVerticle(MyVerticle.class.getName(), new DeploymentOptions().setConfig(config), deploy -> {
			if (deploy.failed()) {
				System.err.println(String.format("Failed to start kafka consumer verticle, ex: %s", deploy.cause()));
				vertx.close();
				return;
			}
			System.out.println("kafka consumer verticle started");
		});
	}
}
