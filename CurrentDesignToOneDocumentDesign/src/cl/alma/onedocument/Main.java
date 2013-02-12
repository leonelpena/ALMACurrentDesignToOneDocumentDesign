package cl.alma.onedocument;

import java.net.UnknownHostException;
import java.util.concurrent.LinkedBlockingQueue;

import com.mongodb.DBObject;

public class Main {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		LinkedBlockingQueue<DBObject> queue = new LinkedBlockingQueue<DBObject>();
		
		MongoManager mongoManager = null;
		try {
			//mongoManager = new MongoManager("localhost",
			mongoManager = new MongoManager("mongo-r1.osf.alma.cl",
					"OneDocumentPerComponentPerDay", "monitorData");
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
			
			// Once exportData() has been executed and the queue is empty 
			// we interrupt the MongoManager thread
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
