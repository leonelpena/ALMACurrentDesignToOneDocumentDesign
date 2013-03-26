package cl.alma.onedocument;

import java.net.UnknownHostException;
import java.util.concurrent.LinkedBlockingQueue;

import com.mongodb.DBObject;

public class Main {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		LinkedBlockingQueue<DBObject> queue = 
				new LinkedBlockingQueue<DBObject>(500000);
		
		MongoManager mongoManager = null;
		try {
			//mongoManager = new MongoManager("localhost",
			mongoManager = new MongoManager("mongo-r1.osf.alma.cl",
					"OneMonitorPointPerDayPerDocument", "monitorData");
			mongoManager.setQueue(queue);
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
		}

		Thread hiloMongo = new Thread(mongoManager);
		hiloMongo.start();

		try {
			Query query = new Query("mongo-r1.osf.alma.cl", "MONDB",
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
			hiloMongo.interrupt();
			
		} catch (UnknownHostException e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

}
