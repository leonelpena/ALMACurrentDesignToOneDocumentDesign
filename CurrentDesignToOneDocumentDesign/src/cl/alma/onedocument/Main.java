package cl.alma.onedocument;

import java.net.UnknownHostException;
import java.util.concurrent.LinkedBlockingQueue;

import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

public class Main {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		LinkedBlockingQueue<DBObject> queue = 
				new LinkedBlockingQueue<DBObject>(500000);
		
		Mongo mongo = null;
		DB database = null;
		try {
			//mongo = new Mongo("localhost");
			mongo = new Mongo("mongo-r1.osf.alma.cl");
			database = mongo.getDB("OneMonitorPointPerDayPerDocument");

			//mongoManager = new MongoManager("mongo-r1.osf.alma.cl",
					//"OneMonitorPointPerDayPerDocument");
			//mongoManager.setQueue(queue);
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
			System.exit(-1);
		}
		
		MongoManager.setConnection(mongo, database);
		
		// Launching the consumer threads
		Thread[] consumers = new Thread[20];
		for (int i=0; i<20; i++) {
			consumers[i] = new Thread(MongoManager.mongoManagerFactory(queue));
			consumers[i].start();
		}

		Query query = null;
		try {
			query = new Query("mongo-r1.osf.alma.cl", "MONDB",
					"monitorPoints");
			query.setQueue(queue);
			query.exportData();
			
			// Interrupting the MongoManager thread once have been consumed
			// all samples in the queue
			while (!queue.isEmpty()) {
				try {
					Thread.sleep(3000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			
			// Stopping the consumers
			for (int i=0; i<20; i++) {
				consumers[i].interrupt();
			}
			
		} catch (UnknownHostException e) {
			e.printStackTrace();
			System.exit(-1);
		} finally {
			
			if (mongo!=null) {
				mongo.close();
			}
			
		}
	}

}
