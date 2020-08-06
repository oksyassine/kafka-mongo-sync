package com.mongodatab.consumer;


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

import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;

public class KafkaSimpleConsumer {


    public static void main(String[] args) {
        //create kafka consumer
        Properties properties = new Properties();
        //23.97.192.20 //setting consumer's properties 
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "23.97.192.20:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "my-first-consumer-group");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        Consumer<String, String> consumer = new KafkaConsumer(properties);
        //subscribe to topic
        consumer.subscribe(Collections.singleton("kafka-topic"));
        MongoClient mongoClient = MongoClients.create();
        //poll the -record from the topic
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {

            	System.out.println("Message received: " + record.value());
            	//convert string record to a Json object
            	JSONObject jsonObject=new JSONObject(record.value());
            	//extract string fields from from the jsonobject
            	String op=jsonObject.optString("op");
            	String res=jsonObject.getString("res");
            	String ns=jsonObject.getString("ns");
            	String to=jsonObject.optString("to","{'db':'test','coll':'col1'}");
            	String doc=jsonObject.optString("doc","{'value':'empty'}");
            	String key=jsonObject.optString("key","{'value':'empty'}");
            	String updatedesc=jsonObject.optString("updatedesc","{'value':'empty'}");

            	//contructing the ChangestreamDocument
            	ChangeStreamDocument<Document> ne=new ChangeStreamDocument<Document>(OperationType.fromString(op),BsonDocument.parse(res), 
            			new BsonDocument().parse(ns), 
            			new BsonDocument().parse(to), 
            			new Document().parse(doc), 
            			new BsonDocument().parse(key), new BsonTimestamp(jsonObject.getLong("cluster")), null, 
            			new BsonInt64(jsonObject.optInt("Txn",0)) , new BsonDocument().parse(jsonObject.optString("Lsid","{'null':1}")));

            	//getting informations related to the database
            	MongoNamespace nms= ne.getNamespace();

            	//performing the requested operation 
            	if(op.matches("insert")) {
            		System.out.println("insert operation");
            		MongoDatabase database = mongoClient.getDatabase(nms.getDatabaseName());
            		MongoCollection<Document> collection = database.getCollection(nms.getCollectionName());
            		System.out.println(mongoClient +"  "+collection);
            		Document document=ne.getFullDocument();
            		collection.insertOne(document);
            	}
            	else if(op.matches("delete")) {
            		System.out.println("delete operation");
            		MongoDatabase database = mongoClient.getDatabase(nms.getDatabaseName());
            		MongoCollection<Document> collection = database.getCollection(nms.getCollectionName());
            		System.out.println(mongoClient +"  "+collection);
            		Bson filter = Filters.eq(ne.getDocumentKey().get("_id"));
            		collection.deleteOne(filter);           		
            	}
            	else if(op.matches("update")) {
            		System.out.println("update operation");
            		MongoDatabase database = mongoClient.getDatabase(nms.getDatabaseName());
            		MongoCollection<Document> collection = database.getCollection(nms.getCollectionName());
            		System.out.println(mongoClient +"  "+collection);
            		Bson filter = Filters.eq(ne.getDocumentKey().get("_id"));
            		collection.deleteOne(filter); 
            		Document document=ne.getFullDocument();
            		collection.insertOne(document);
            	}
            	else if(op.matches("rename")) {
            		System.out.println("rename operation");
            		MongoDatabase database = mongoClient.getDatabase(nms.getDatabaseName());
            		MongoCollection<Document> collection = database.getCollection(nms.getCollectionName());
            		System.out.println(mongoClient +"  "+collection);
            		collection.renameCollection(ne.getDestinationNamespace());
            	}else if(op.matches("drop")){
            		System.out.println("drop operation");
            		MongoDatabase database = mongoClient.getDatabase(nms.getDatabaseName());
            		MongoCollection<Document> collection = database.getCollection(nms.getCollectionName());
            		System.out.println(mongoClient +"  "+collection);
            		collection.drop();
            	}

            }
            //commit offset
            consumer.commitAsync();
        }
    }
} 