/**
 * 
 */
package Local;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.json.JSONObject;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;

/**
 * @author Ardouz
 *
 */
public class ProducerLocal extends Thread {
	private static KafkaProducer<Integer, String> producer;
	private static final String TOPIC = "ardouz-1";
	private static final String KAFKA_SERVER_URL = "ip server";
	private static final int KAFKA_SERVER_PORT = 9092;
	private static final String CLIENT_ID = "ARLocalProducer";
	private static String line;
	public static Document producer_local=new Document();
	private static int messageNo = 1;
	private static String message;
	private static Properties properties;
	private static MongoCursor<ChangeStreamDocument<Document>> cursor;
	private static ChangeStreamDocument<Document> next;
	private static BsonDocument resumeToken;

	static void produce(ChangeStreamDocument<Document> next) {
		// Construct the json object with the parameters of the ChangeStreamDocument
		// Constructor Object.
		// System.out.println(next.toString());
		
		JSONObject cons = new JSONObject();
		cons.put("op", next.getOperationType().getValue());
		cons.put("res", next.getResumeToken().toString());
		if (next.getNamespaceDocument() != null)
			cons.put("ns", next.getNamespaceDocument().toString());
		if (next.getDestinationNamespaceDocument() != null)
			cons.put("to", next.getDestinationNamespaceDocument().toString());
		if (next.getFullDocument() != null)
			cons.put("doc", next.getFullDocument().toJson());
		if (next.getDocumentKey() != null)
			cons.put("key", next.getDocumentKey().toString());
		if (next.getClusterTime() != null)
			cons.put("cluster", next.getClusterTime().getValue());
		
		/*
		 * if(next.getUpdateDescription()!=null) { cons.put("f.rem",
		 * next.getUpdateDescription().getRemovedFields()); cons.put("f.upd",
		 * next.getUpdateDescription().getUpdatedFields().toString()); }
		 */
		if (next.getTxnNumber() != null)
			cons.put("Txn", next.getTxnNumber().toString());
		if (next.getLsid() != null)
			cons.put("Lsid", next.getLsid().toString());
		message = cons.toString();
		if(next.getDocumentKey() != null) {
		producer_local = next.getFullDocument();}
		//System.out.println("test "+producer_local.toString());
				//System.out.println("testttttttttt "+next.getFullDocument().getObjectId(cons.get("key")).get().toString());
		// Send the String format of the json object
		try {
			
			producer.send(new ProducerRecord<Integer, String>(TOPIC, messageNo, message)).get();
		} catch (InterruptedException | ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Sent message by local producer: (" + messageNo + ", " + message + ")");
		++messageNo;
	}

	@Override
	public void run() {
		// Construct a KafkaProducer with the following properties
		properties = new Properties();
		properties.put("bootstrap.servers", KAFKA_SERVER_URL + ":" + KAFKA_SERVER_PORT);
		properties.put("client.id", CLIENT_ID);
		properties.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producer = new KafkaProducer<Integer, String>(properties);
		// Create a new Mongo Client Object to listen for change streams for all
		// databases in the mongo server
		ConnectionString conx = new ConnectionString("mongodb://localhost:27017/db");
		MongoClient mongoClient = MongoClients.create(conx);
		// MongoDatabase database = mongoClient.getDatabase("database");
		// MongoCollection<Document> collection = database.getCollection("collection");
		
		File file = new File("resume.txt");
		// Create the file for the resume token if it doesn't exist
		try {
			file.createNewFile();
			// System.out.println("success");
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}

		while (true) {
			try {
				// Read the file to get the resume token
				BufferedReader br = new BufferedReader(new FileReader(file));
				line = br.readLine();
				br.close();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			if (line == null) {
				// If the file doesn't contain any resume token, then listen for the first event
				cursor = mongoClient.watch().fullDocument(FullDocument.UPDATE_LOOKUP).iterator();
				next = cursor.next();
				// After getting the first event, we produce the message and send it to the
				// kafka broker
				produce(next);
				// After that, we save the resume token into the file.
				resumeToken = next.getResumeToken();
				try (PrintStream ps = new PrintStream(file)) {
					ps.print(resumeToken);
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				// Create a new instance of the BsonDocument for the resume token already saved
				// in the file
				new BsonDocument();
				BsonDocument res = BsonDocument.parse(line);
				// Listen for the event that come after the event with the corresponding resume
				// token
				cursor = mongoClient.watch().fullDocument(FullDocument.UPDATE_LOOKUP).startAfter(res).iterator();
				next = cursor.next();
				produce(next);
				// Get the resume token from the new event and save it on the file
				resumeToken = next.getResumeToken();
				try (PrintStream ps = new PrintStream(file)) {
					ps.println(resumeToken);
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		}
	}

}
