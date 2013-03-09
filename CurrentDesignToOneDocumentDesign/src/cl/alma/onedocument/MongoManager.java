package cl.alma.onedocument;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.util.JSON;

public class MongoManager implements Runnable {
	
	public static final int N_MONITOR_POINTS = 100000;
	public static final String NOT_ASSIGNED = "na";
	
	private static final Logger log = Logger.getLogger(MongoManager.class);
	private Mongo mongo;
	private DB database;
	private HashMap<String, DBCollection> mongoCollections;
	//private HashMap<Integer, DBCollection> mongoCollections;
	private DBCollection multipleCollection;
	
	private DBCollection collection;
	private LinkedBlockingQueue<DBObject> queue;
	
	private HashMap<String, Boolean> createdDocuments;

	public MongoManager(String host, String dbname, String coll) 
					throws UnknownHostException {

		mongo = new Mongo(host);
		database = mongo.getDB(dbname);
		mongoCollections = new HashMap<String, DBCollection>(3);
		//mongoCollections = new HashMap<Integer, DBCollection>(70);
		//collection = database.getCollection("monitorData");
		collection = database.getCollection(coll);
		createdDocuments = new HashMap<String, Boolean>(N_MONITOR_POINTS);
	}
	
	public void setQueue(LinkedBlockingQueue<DBObject> queue) {
		this.queue = queue;
	}

	public DBCollection getCollection(DocumentID id) {

		String key = Integer.toString(id.getMonth()) + "_" + 
				Integer.toString(id.getYear());
		
		if (mongoCollections.containsKey(key))
			return mongoCollections.get(key);

		BasicDBObject index = null;
		BasicDBObject shardKey = null;
		if (!database.collectionExists("monitorData_"+key)) {

			// Setting the index
			index = new BasicDBObject("metadata.date", 1);
			index.append("metadata.monitorPoint", 1);
			index.append("metadata.antenna", 1);
			index.append("metadata.component", 1);
			
			// Setting the shard key
			shardKey = new BasicDBObject("metadata.date", 1);
			shardKey.append("metadata.antenna", 1);
		}

		// if the collection does not exist, it will be created automatically
		DBCollection c = database.getCollection("monitorData_"+key);
		mongoCollections.put(key, c);
		
		if (index!=null) {
			//c.createIndex(index);
			c.ensureIndex(index);
		}
		
		if (shardKey!=null) {
			//c.sh
		}
			
		return c;
	}

	public void upsert(Metadata metadata, int hour, int minute,
			int second, String value) {

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

		// Monitor data value to update
		String attribute = "hourly." + Integer.toString(hour) + "." + 
					Integer.toString(minute) + "." + Integer.toString(second);

		BasicDBObject updateDocument = new BasicDBObject().append("$set",
				new BasicDBObject().append(attribute, value));

		//System.out.println("Document: "+document);
		//System.out.println("Update Document: "+updateDocument);

		//collection.update(document, updateDocument, true, false);
		
		// Codigo para usar colecciones mensuales.
		multipleCollection = getCollection(metadata.getDocumentID());
		multipleCollection.update(document, updateDocument, true, false);
	}
	
	public void upsert(Sample sample) {

		// Monitor data value to update
		String attribute = "hourly." + sample.getHour() + "." + 
					sample.getMinute() + "." + sample.getSecond();

		Metadata metadata = sample.getMetadata();
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
				new BasicDBObject().append(attribute, sample.getValue()));

		//System.out.println("Document: "+document);
		//System.out.println("Update Document: "+updateDocument);

		//collection.update(document, updateDocument, true, false);
		
		// Codigo para usar colecciones mensuales.
		multipleCollection = getCollection(metadata.getDocumentID());
		multipleCollection.update(document, updateDocument, true, false);
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

	public DBObject preAllocate(Metadata metadata) {

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

		//List<BasicDBObject> hourly = new ArrayList<BasicDBObject>(24*60*60);
		/*StringBuilder hourly = new StringBuilder("{ 'hourly' :");

		for (int hour=0; hour<24; hour++) {

			hourly.append("{'"+hour+"' : ");

			for (int minute=0; minute<60; minute++) {
				
				hourly.append("{'"+minute+"' : ");
				
				for (int second=0; second<60; second++) {

					hourly.append("{'"+second+"' : {");

					// Monitor data value to preallocate
					String attribute = "hourly." + Integer.toString(hour) + "." + 
								Integer.toString(minute) + "." + Integer.toString(second);

					BasicDBObject updateDocument = new BasicDBObject().append("$set",
							new BasicDBObject().append(attribute, value));
					
					JSON.parse(s)
					
					hourly.append("}");
				}
			}
			
			hourly.append("}");
		}*/
		/*Map<Integer,Object> hours = new HashMap<Integer,Object>(24);
		Map<Integer,Object> minutes;// = new HashMap<Integer,Object>(60);
		Map<Integer,String> seconds;// = new HashMap<Integer,Object>(60);

		for (int h=0; h<24; h++) {

			minutes = new HashMap<Integer,Object>(60);
			hours.put(h, minutes);
			for (int m=0; m<60; m++) {
				
				seconds = new HashMap<Integer,String>(60);
				minutes.put(m, seconds);
				for (int s=0; s<60; s++) {

					seconds.put(s, NOT_ASSIGNED);
					
					// Monitor data value to preallocate
					//String attribute = "hourly." + Integer.toString(hour) + "." + 
						//		Integer.toString(minute) + "." + Integer.toString(second);

					//BasicDBObject updateDocument = new BasicDBObject().append("$set",
						//	new BasicDBObject().append(attribute, value));
				}
			}
		}
		JSON.parse;
			
		return hours;*/

		BasicDBObjectBuilder hourly = null;
		BasicDBObjectBuilder hours = null;
		BasicDBObjectBuilder minutes = null;
		BasicDBObjectBuilder seconds = null;

		hours = new BasicDBObjectBuilder();
		for (int h=0; h<24; h++) {

			minutes = new BasicDBObjectBuilder();
			for (int m=0; m<60; m++) {

				seconds = new BasicDBObjectBuilder();
				for (int s=0; s<60; s++) {

					seconds.add(Integer.toString(s), NOT_ASSIGNED);
				}

				if (seconds!=null && !seconds.isEmpty()) {
					minutes = new BasicDBObjectBuilder();
					minutes.add(Integer.toString(m), seconds);
				}
				seconds = null;
			}

			if (minutes!=null && !minutes.isEmpty()) {
				hours = new BasicDBObjectBuilder();
				hours.add(Integer.toString(h), minutes);
			}
			minutes = null;
		}

		if (hours!=null && !hours.isEmpty()) {
			hourly = new BasicDBObjectBuilder();
			hourly.add("hourly", hours);
		}

		if (hourly!=null && !hourly.isEmpty())
			return hourly.get();

		return null;
	}

	public boolean isDocumentCreated(DocumentID id) {
		if (createdDocuments.containsKey(id.toString()))
			return true;

		DBCollection coll = getCollection(id);
		DBObject doc = coll.findOne(new BasicDBObject("_id",id.toString()));
		if (doc!=null) {
			createdDocuments.put(id.toString(), true);
			return true;
		}

		return false;
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

		if (mongoManager.isDocumentCreated(documentID)) {
			
		}
		
		mongoManager.upsert(metadata, 14, 4, 6, "12345");
	}

	/*
	@Override
	public void run() {
		int cont=0, error=0;
		try {
			while (true) {
				DBObject object = queue.take();

				//Set<String> mySet = object.keySet();
				Map<String, Object> myMap = object.toMap();
				//System.out.println("Set: "+mySet);
				

				//if (myMap.get("date") instanceof Date) {
					//System.out.println("Es instacia de DATE!!");
				//} else {
					//System.out.println("NO lo es!");
				//}
				
				Calendar calendar = Calendar.getInstance();
				try {
					calendar.setTime((Date)myMap.get("date"));
				} catch (NullPointerException e) {
					System.err.println("Object: "+myMap);
				}
			    
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
			
		} catch (Throwable e) {
			error++;
			//System.err.println("Exception caught: "+e.getMessage());
			log.error("Exception caught: "+e.getMessage());
		} finally {
			close();
			System.out.println("Registros insertados: "+cont);
			System.out.println("Errores: "+error);
		}
	}*/
	
	@Override
	public void run() {
		SynchronousQuery synQuery = null;
		try {
			synQuery = new SynchronousQuery("mongo-r1.osf.alma.cl", "MONDB",
					"monitorPoints");
		} catch (UnknownHostException e1) {
			System.exit(-1);
		}

		DBCursor cursor = synQuery.exportData();

		int cont=0, error=0;
		while (cursor.hasNext()) {
			try {
				//DBObject object = queue.take();
				DBObject object = cursor.next();
	
				//Set<String> mySet = object.keySet();
				Map<String, Object> myMap = object.toMap();
				//System.out.println("Set: "+mySet);
				
	
				/*if (myMap.get("date") instanceof Date) {
					//System.out.println("Es instacia de DATE!!");
				} else {
					//System.out.println("NO lo es!");
				}*/
				
				Calendar calendar = Calendar.getInstance();
				try {
					calendar.setTime((Date)myMap.get("date"));
				} catch (NullPointerException e) {
					log.error("NullPointerException: "+myMap);
					error++;
				}
			    
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
			    // Component and subcomponent are put together
			    String component = null;
			    
			    // If the old ComponentName length is three it means there is
			    // just a component, however, if the length is four
			    // there is a component and subcomponent
			    if (oldComponentName.length==3) {
			    	component = oldComponentName[2];
			    } else if (oldComponentName.length==4) {
			    	component = oldComponentName[2]+"/"+oldComponentName[3];
		    	} else {
		    		log.error("Error detected in component name: "+myMap.get("componentName"));
		    	}

			    //if (oldComponentName.length>3) {
			    	//component = oldComponentName[2]+"/"+oldComponentName[3];
			    //} else {
			    	//component = oldComponentName[2];
			    //}
			    
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
			} catch (Throwable e) {
				error++;
				log.error("Exception caught: "+e.getMessage());
				log.error(Arrays.toString(e.getStackTrace()));
			}
		}

		synQuery.closeCursor();
		close();
		log.info("Registros insertados: "+cont);
		log.info("Errores: "+error);
	}	
	
}
