package cl.alma.onedocument;

import java.net.UnknownHostException;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.concurrent.LinkedBlockingQueue;

import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.Bytes;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

public class Query {
	private Mongo _mongo;
	private DB _database;
	private DBCollection _collection;
	private LinkedBlockingQueue<DBObject> queue;

	public Query(String host, String dbname, String collection) 
					throws UnknownHostException {

		_mongo = new Mongo(host);
		_database = _mongo.getDB(dbname);
		_collection = _database.getCollection(collection);
	}

	public void setQueue(LinkedBlockingQueue<DBObject> queue) {
		this.queue = queue;
	}

	public void exportData() {
		// The months in Gregorian Calendar start with 0
		Calendar startDate = new GregorianCalendar(2012, 9-1, 01, 0, 0, 0);
		Calendar endDate = new GregorianCalendar(2012, 9-1, 30, 23, 59, 59);

		/*	*/
		BasicDBObject query = new BasicDBObject("date", new BasicDBObject(
				"$gte", startDate.getTime()).append("$lte", endDate.getTime()));
		query.append("componentName", "CONTROL/DV10/FrontEnd/Cryostat");
		query.append("monitorPointName", "GATE_VALVE_STATE");
		//query.append("componentName", "CONTROL/DV16/LLC");
		//query.append("monitorPointName", "POL_MON4");

		// Used only when the query take more than 10 minutes.
		_collection.addOption(Bytes.QUERYOPTION_NOTIMEOUT);

		/*	*/

		// Registros utilizados para probar la diferencia de la zona horaria 
		// local con la del servidor de mongo
		//BasicDBObject query = new BasicDBObject("_id", new ObjectId("50528be325d8b6dfbafd7ac2"));
		//BasicDBObject query = new BasicDBObject("_id", new ObjectId("50529496a310ecc5da59531c"));

		DBCursor cursor = _collection.find(query);

		//System.out.println("Collections: "+_database.getCollectionNames());
		
		//System.out.println("Error: "+_database.getLastError());
		try {
			int cont=0;
			while(cursor.hasNext()) {

				try {
					queue.put(cursor.next());
					cont++;
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			System.out.println("Total: "+cont);
		} finally {
			cursor.close();
		}
	}
}
