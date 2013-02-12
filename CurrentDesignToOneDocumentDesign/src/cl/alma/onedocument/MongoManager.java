package cl.alma.onedocument;

import java.net.UnknownHostException;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

public class MongoManager implements Runnable {
	private Mongo mongo;
	private DB database;
	private HashMap<String, DBCollection> mongoCollections;
	private DBCollection collection;
	private LinkedBlockingQueue<DBObject> queue;

	public MongoManager(String host, String dbname, String coll) 
					throws UnknownHostException {

		mongo = new Mongo(host);
		database = mongo.getDB(dbname);
		mongoCollections = new HashMap<String, DBCollection>(70);
		//collection = database.getCollection("monitorData");
		collection = database.getCollection(coll);
	}
	
	public void setQueue(LinkedBlockingQueue<DBObject> queue) {
		this.queue = queue;
	}
	
	public DBCollection getCollection(DocumentID id) {
		//String key = id.getAntenna() + DocumentID.SEPARATOR + id.getComponent();
		String key = id.toString();

		if (mongoCollections.containsKey(key))
			return mongoCollections.get(key);

		// if the collection does not exist, it will be created automatically
		DBCollection c = database.getCollection("monData_"+key);
		mongoCollections.put(key, c);
		return c;
	}

	public void upsert(Metadata metadata, int hour, int minute,
			int second, String value) {

		// Monitor data value to update
		String attribute = "hourly." + Integer.toString(hour) + "." + 
					Integer.toString(minute) + "." + Integer.toString(second);

		//collection = getCollection(metadata.getDocumentID());

		BasicDBObject document = new BasicDBObject().append("_id",
				metadata.getDocumentID().toString());

		document.append("metadata", new BasicDBObject().append(
				//"date", metadata.getDocumentID().getDate().getTime()).append(
				"date", metadata.getDocumentID().getStringDate()).append(
				"antenna", metadata.getDocumentID().getAntenna()).append(
				"component", metadata.getDocumentID().getComponent()).append(
				"property", metadata.getProperty()).append(
				"monitorPoint", metadata.getDocumentID().getMonitorPoint()).append(
				"location", metadata.getLocation()).append(
				"serialNumber", metadata.getSerialNumber()).append(
				"index", metadata.getIndex()).append(
				"sampleTime", metadata.getSampleTime())
		);

		BasicDBObject updateDocument = new BasicDBObject().append("$set",
				new BasicDBObject().append(attribute, value));

		//System.out.println("Document: "+document);
		//System.out.println("Update Document: "+updateDocument);

		collection.update(document, updateDocument, true, false);
	}

	/*public void update(DocumentID id, int hour, int minute, int second, 
			String value) {

		// Monitor data value to update
		String attribute = "hourly." + Integer.toString(hour) + "." + 
					Integer.toString(minute) + "." + Integer.toString(second);

		collection = getCollection(id);

		BasicDBObject newDocument = new BasicDBObject().append("$set",
				new BasicDBObject().append(attribute, value));
		collection.update(new BasicDBObject().append("_id", id.toString()),
				newDocument);
		
		// In the cases where there is not a document created for a day and
		// the TMC server needs it, it will be created by the own TMC server.
		// This is not a optimal situation because it decrease the performance   
		// (the reasons are explained in the wiki's project).
		// The preAllocate method is aimed to solve this issue, but as been 
		// said, it decrease the performance if is used inside the TMC server.
	}*/

	public void close() {
		mongo.close();
	}

	public void preAllocate(DocumentID id) {
		System.out.println("Pre-allocating a document");
	}

	/**
	 * Method for testing MongoManager class
	 * @param args
	 */
	public static void main(String[] args) {
		MongoManager mongoManager = null;
		try {
			mongoManager = new MongoManager("localhost", 
					"OneDocumentPerComponentPerDay", "monitorData");
		} catch (UnknownHostException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		
		Calendar date = new GregorianCalendar(2012, 9, 30, 0, 0, 0);
		//DocumentID documentID = new DocumentID(date, "CM02", "LLC", "POL_MON4");
		DocumentID documentID = new DocumentID(2012, 9, 30, "CM02", "LLC", "POL_MON4");
		Metadata metadata = new Metadata(documentID, "ASDF Property", "TFING",
				"as76d6fh", 5, 2);
		
		mongoManager.upsert(metadata, 14, 4, 6, "12345");
	}

	@Override
	public void run() {
		int cont=0;
		try {
			while (true) {
				DBObject object = queue.take();

				//Set<String> mySet = object.keySet();
				Map<String, Object> myMap = object.toMap();
				//System.out.println("Set: "+mySet);
				

				/*if (myMap.get("date") instanceof Date) {
					//System.out.println("Es instacia de DATE!!");
				} else {
					//System.out.println("NO lo es!");
				}*/
				
				Calendar calendar = Calendar.getInstance();
			    calendar.setTime((Date)myMap.get("date"));
			    
			    // ************************************************ //
			    // Se añaden las tres horas de diferencia 			//
			    // con el servidor de mongo.						//
			    // ************************************************	//
			    calendar.add(Calendar.HOUR, 3);

			    int year = calendar.get(Calendar.YEAR);
			    int month = calendar.get(Calendar.MONTH)+1;
			    int day = calendar.get(Calendar.DAY_OF_MONTH);
			    
			    int hour = calendar.get(Calendar.HOUR_OF_DAY);
			    int minute =  calendar.get(Calendar.MINUTE);
			    int second =  calendar.get(Calendar.SECOND);
			    
			    //System.out.println("Map: "+myMap+",\n year: "+year+", month: "+
			    	//	month+", day: "+day+", hour: "+hour+", minute: "+minute+
			    		//", second: "+second);
			    
			    // We need to split the componentName that comes from the old schema.
			    // The format is "CONTROL/DV10/FrontEnd/Cryostat".
			    // We extract the antenna, component and subcomponent 
			    // from it.
			    String[] oldComponentName = ((String)myMap.get("componentName")).split("/");
			    String antenna = oldComponentName[1];
			    String component = null;
			    
			    // If the old ComponentName length is three it means there is
			    // just a component, however, if the length is major than three
			    // there is a component and subcomponent
			    if (oldComponentName.length>3) {
			    	component = oldComponentName[2]+"/"+oldComponentName[3];
			    } else {
			    	component = oldComponentName[2];
			    }
			    
			    String property = (String)myMap.get("propertyName");
			    String monitorPoint = (String)myMap.get("monitorPointName");
			    String location = (String)myMap.get("location");
			    String serialNumber = (String)myMap.get("serialNumber");
			    String monitorValue = (String)myMap.get("monitorValue");
			    int index = Integer.parseInt(myMap.get("index").toString());
			    int sampleTime = 0;

				DocumentID documentID = new DocumentID(year, month, day,
						antenna, component, monitorPoint);
				//Metadata metadata = new Metadata(documentID, "ASDF Property", "TFING",
					//	"as76d6fh", 5, 2);
				Metadata metadata = new Metadata(documentID, property, 
						location, serialNumber, sampleTime, index);
				
				//mongoManager.upsert(metadata, 14, 4, 6, "12345");
				upsert(metadata, hour, minute, second, monitorValue);
				cont++;
				//insert(record);
			}
		} catch (InterruptedException e) {
			
		} finally {
			close();
			System.out.println("Registros insertados: "+cont);
		}
	}
}
