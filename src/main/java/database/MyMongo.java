package database;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;

public class MyMongo extends AbstractVerticle{

	public void start() {
		JsonObject config = new JsonObject()
		        .put("connection_string", "mongodb://localhost:27017");
	}
}
