package cl.alma.performancetest;

import java.net.UnknownHostException;
import java.util.Calendar;
import java.util.GregorianCalendar;

import cl.alma.onedocument.DocumentID;
import cl.alma.onedocument.Metadata;
import cl.alma.onedocument.MongoManager;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;


/**
 * This code is aimed to test the performance of the design "One monitor point 
 * per day per document".
 * 
 * The test consist of create 100.000 document per day for 70 antennas 
 * for 30 days
 *  
 * @author Leonel Peña <leo.dhpl@gmail.com>
 *
 */
public class Main {

	/* 
	public static final int ANTENNAS = 70;
	public static final int COMPONENT_PER_ANTENNA = 41; 
	public static final int MONITOR_POINTS_PER_COMPONENT = 35;
	public static final int DAYS = 30;
	public static final int MONTH = 1;
	public static final int YEAR = 2013;
	/*  */
	
	/*  */
	public static final int ANTENNAS = 70;
	public static final int COMPONENT_PER_ANTENNA = 41;
	public static final int MONITOR_POINT_PER_COMPONENT = 35;
	public static final int DAYS = 1;
	public static final int MONTH = 2;
	public static final int YEAR = 2013;
	/*  */
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Mongo mongo = null;
		DB database = null;
		try {
			//mongo = new Mongo("localhost");
			mongo = new Mongo("mongo-r2.osf.alma.cl");
			database = mongo.getDB("OneDocumentPerformanceTest");
		} catch (UnknownHostException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		
		MongoManager.setConnection(mongo, database);
		MongoManager mongoManager = MongoManager.mongoManagerFactory(null);
		DBCollection collection = database.getCollection("monitorData_"+MONTH);
		
		for (int day=1; day<=DAYS; day++) {
			for (int antenna=1; antenna<=ANTENNAS; antenna++) {
				for (int component=1; component<=COMPONENT_PER_ANTENNA; component++) {
					for (int monitorPoint=1; monitorPoint<=MONITOR_POINT_PER_COMPONENT; monitorPoint++) {
						
						DocumentID doc = new DocumentID(YEAR, MONTH, day,
								"Antenna_"+antenna, "Component_"+component, 
								"MP_"+monitorPoint);
						
						Metadata meta = new Metadata(doc, "asdf", "AOS", 
								"lkjhg", 0, 1);
						
						Calendar tStart = new GregorianCalendar(YEAR, MONTH, 
								day, 0, 0, 0);
						
						// 5 is the size of the values for the monitoring point 
						//collection.insert(mongoManager.preAllocate(meta, tStart.getTime(), 5));
						//collection.insert(mongoManager.preAllocate(meta,5));
						collection.insert(MongoManager.getPreallocatedDocument(meta,5));
					}
				}
			}
		}
		
		mongo.close();
	}

}
