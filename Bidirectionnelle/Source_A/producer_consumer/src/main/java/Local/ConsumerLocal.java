/**
 * 
 */
package Local;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.json.JSONObject;

import com.mongodb.ConnectionString;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;

/**
 * @author Ardouz
 *
 */
public class ConsumerLocal extends Thread {
	public static Document consumer_local=new Document();
	@Override
	public void run() {
		
	       Properties properties = new Properties();
	        //23.97.192.20 //setting consumer's properties 
	        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "ip server:9092");
	        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-2-group");
	        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
	        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
	        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
	        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

	        Consumer<String, String> consumer = new KafkaConsumer(properties);
	        //subscribe to topic
	        consumer.subscribe(Collections.singleton("ardouz-2"));
	        ConnectionString conx = new ConnectionString("mongodb://localhost:27017/db");
	        MongoClient mongoClient = MongoClients.create(conx);
	        //MongoClient mongoClient = MongoClients.create();
	        //poll the -record from the topic
	        while (true) {
	            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
	            for (ConsumerRecord<String, String> record : records) {

	            	System.out.println("Message received to local consummer: "+ "topic :" +record.topic().toString() + record.value());
	            	//convert string record to a Json object
	            	JSONObject jsonObject=new JSONObject(record.value());
	            	//extract string fields from from the jsonobject
	            	String op=jsonObject.optString("op");
	            	String res=jsonObject.getString("res");
	            	String ns=jsonObject.getString("ns");
	            	String to=jsonObject.optString("to","{'db':'database','coll':'collections'}");
	            	String doc=jsonObject.optString("doc","{'value':'empty'}");
	            	String key=jsonObject.optString("key","{'value':'empty'}");
	            	String updatedesc=jsonObject.optString("updatedesc","{'value':'empty'}");

	            	new BsonDocument();
					new BsonDocument();
					new Document();
					new BsonDocument();
					new BsonDocument();
					//contructing the ChangestreamDocument
	            	ChangeStreamDocument<Document> ne=new ChangeStreamDocument<Document>(OperationType.fromString(op),BsonDocument.parse(res), 
	            			BsonDocument.parse(ns), 
	            			BsonDocument.parse(to), 
	            			Document.parse(doc), 
	            			BsonDocument.parse(key), new BsonTimestamp(jsonObject.getLong("cluster")), null, 
	            			new BsonInt64(jsonObject.optInt("Txn",0)) , BsonDocument.parse(jsonObject.optString("Lsid","{'null':1}")));

	            	//getting informations related to the database
	            	MongoNamespace nms= ne.getNamespace();
	            	consumer_local =ne.getFullDocument();
	            	
	            	//performing the requested operation 
	            	if(op.matches("insert")) {
	            		if ((Local.ProducerLocal.producer_local.equals(consumer_local))) {
		    				System.out.println("Same Don't Send");

		    			}else
		    			{
	            		System.out.println("insert operation");
	            		MongoDatabase database = mongoClient.getDatabase(nms.getDatabaseName());
	            		System.out.println("hahia lbase d donnee "+nms.getDatabaseName().toString());
	            		MongoCollection<Document> collection = database.getCollection(nms.getCollectionName());
	            		Document document=ne.getFullDocument();
	            		collection.insertOne(document);
	            	}}
	            	
	            	else if(op.matches("update")) {
	            		if (( Local.ProducerLocal.producer_local.equals(consumer_local))) {
		    				System.out.println("Same Don't Send");

		    			}else
		    			{
	            		System.out.println("update operation");
	            		MongoDatabase database = mongoClient.getDatabase(nms.getDatabaseName());
	            		MongoCollection<Document> collection = database.getCollection(nms.getCollectionName());
	            		System.out.println(mongoClient +"  "+collection);
	            		Bson filter = Filters.eq(ne.getDocumentKey().get("_id"));
	            		collection.deleteOne(filter); 
	            		Document document=ne.getFullDocument();
	            		collection.insertOne(document);
	            	}}
	            	else if(op.matches("rename")) {
	            		if (( Local.ProducerLocal.producer_local.equals(consumer_local))) {
		    				System.out.println("Same Don't Send");

		    			}else
		    			{
	            		System.out.println("rename operation");
	            		MongoDatabase database = mongoClient.getDatabase(nms.getDatabaseName());
	            		MongoCollection<Document> collection = database.getCollection(nms.getCollectionName());
	            		System.out.println(mongoClient +"  "+collection);
	            		collection.renameCollection(ne.getDestinationNamespace());}
	            	}else if(op.matches("drop")){
	            		System.out.println("drop operation");
	            		MongoDatabase database = mongoClient.getDatabase(nms.getDatabaseName());
	            		MongoCollection<Document> collection = database.getCollection(nms.getCollectionName());
	            		System.out.println(mongoClient +"  "+collection);
	            		collection.drop();
	            	
	            	}
	            	 if(op.matches("delete")) {
	            		System.out.println("delete operation");
	            		MongoDatabase database = mongoClient.getDatabase(nms.getDatabaseName());
	            		MongoCollection<Document> collection = database.getCollection(nms.getCollectionName());
	            		System.out.println(mongoClient +"  "+collection);
	            		Bson filter = Filters.eq(ne.getDocumentKey().get("_id"));
	            		collection.deleteOne(filter);           		
	            	}

	            }
	            //commit offset
	            consumer.commitAsync();
	        }
	    }

}
