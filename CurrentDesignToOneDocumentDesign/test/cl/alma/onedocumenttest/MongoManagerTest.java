package cl.alma.onedocumenttest;

import static org.junit.Assert.*;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

import cl.alma.onedocument.DocumentID;
import cl.alma.onedocument.Metadata;
import cl.alma.onedocument.MongoManager;
import cl.alma.onedocument.Sample;

public class MongoManagerTest {

	private static MongoManager mongo;
	private static Mongo _mongo = null;
	private static DB _database = null;

	@BeforeClass
	public static void setup() {
		
		try {
			_mongo = new Mongo("localhost");
			_database = _mongo.getDB("JUnitTest");
		} catch (UnknownHostException e) {
			fail("Cannot connect to MongoDB instance");
		}
		
		MongoManager.setConnection(_mongo, _database);
		
		mongo = MongoManager.mongoManagerFactory(new LinkedBlockingQueue<DBObject>());
	}
	
	@AfterClass
	public static void clean() {
		/*try {
			mongo = new MongoManager("localhost", "JUnitTest", "MongoManagerTest");
		} catch (UnknownHostException e) {
			fail("Cannot connect to MongoDB instance");
		}*/
		if (mongo!=null)
			mongo.close();
	}
	
	@Test
	public void testGetCollection() {
		DocumentID doc1 = new DocumentID(2012, 12, 30, "CM02", "LLC", "POL_MON4");
		DocumentID doc2 = new DocumentID(2012, 7, 23, "CM10", "LLC", "POL_MON1");
		DocumentID doc3 = new DocumentID(2012, 10, 1, "DV10", "LLC", "POL_MON4");
		
		DBCollection coll1 = mongo.getCollection(doc1);
		DBCollection coll2 = mongo.getCollection(doc2);
		DBCollection coll3 = mongo.getCollection(doc3);

		String[] generatedNames = new String[3];
		generatedNames[0] = coll1.getName();
		generatedNames[1] = coll2.getName();
		generatedNames[2] = coll3.getName();

		String[] expectedNames = new String[3];
		expectedNames[0] = "monitorData_12_2012";
		expectedNames[1] = "monitorData_7_2012";
		expectedNames[2] = "monitorData_10_2012";
		
		assertArrayEquals(expectedNames, generatedNames);
	}

	@Test
	public void testPreAllocate() {
		//fail("Not yet finished");
		
		// Sample time: 5 sec.
		DocumentID documentID = new DocumentID(2012, 9, 30, "DV04", "LLC", "POL_MON4");
		Metadata metadata = new Metadata(documentID, "ASDF Property", "TFING",
				"as76d6fh", 2, MongoManager.DEFAULT_PREALLOCATE_TIME);
		
		Calendar date = new GregorianCalendar(2012, 9, 30, 23, 59, 27);

		DBObject dbObject = mongo.preAllocate(metadata, date.getTime(), 1);
		DBCollection coll = mongo.getCollection(documentID);
		coll.insert(dbObject);
		//System.out.println(com.mongodb.util.JSON.serialize(dbObject));

		fail("Not yet finished");
	}

	@Test
	public void testIsDocumentCreated() {
		DocumentID doc1 = new DocumentID(2012, 10, 1, "CM02", "LLC", "POL_MON1");
		DocumentID doc2 = new DocumentID(2012, 10, 1, "DV10", "LLC", "POL_MON2");
		DocumentID doc3 = new DocumentID(2012, 10, 2, "CM02", "LLC", "POL_MON1");
		DocumentID doc4 = new DocumentID(2012, 10, 2, "CM02", "LLC", "POL_MON1");
		DocumentID doc5 = new DocumentID(2012, 10, 1, "DV10", "LLC", "POL_MON2");
		
		boolean[] actual = new boolean[5];
		actual[0] = mongo.isDocumentCreated(doc1, true);
		actual[1] = mongo.isDocumentCreated(doc2, true);
		actual[2] = mongo.isDocumentCreated(doc3, true);
		actual[3] = mongo.isDocumentCreated(doc4, true);
		actual[4] = mongo.isDocumentCreated(doc5, true);

		boolean[] expected = new boolean[5];
		expected[0] = false;
		expected[1] = false;
		expected[2] = false;
		expected[3] = false; // because the document has not been added to mongo
		expected[4] = false; // because the document has not been added to mongo

		assertTrue(Arrays.equals(expected, actual));
	}
	
	@Test
	public void testRegisterDocumentToBuffer() {
		DocumentID doc1 = new DocumentID(2011, 4, 1, "CM02", "LLC", "POL_MON1");
		DocumentID doc2 = new DocumentID(2010, 2, 1, "DV10", "LLC", "POL_MON3");

		boolean[] expected = new boolean[3];
		boolean[] actual = new boolean[3];

		expected[0] = false;
		actual[0] = mongo.isDocumentCreated(doc1, true);

		mongo.registerDocumentToBuffer(doc1);
		expected[1] = true;
		actual[1] = mongo.isDocumentCreated(doc1, true);

		mongo.registerDocumentToBuffer(doc2);
		expected[2] = true;
		actual[2] = mongo.isDocumentCreated(doc2, true);

		assertTrue(Arrays.equals(expected, actual));
	}

	@Test
	public void testUpsertSample() {
		DocumentID documentID_1 = new DocumentID(2012, 3, 20, "DA41", "LLC", 
				"POL_MON3");
		Metadata metadata_1 = new Metadata(documentID_1, "ASDF Property", "TFING",
				"as76d6fh", 2, MongoManager.DEFAULT_PREALLOCATE_TIME);

		for (int i=0; i<6; i++)
			mongo.upsert(new Sample(metadata_1, 22, 31, 43+i, "6,896"), true);

		DocumentID documentID_2 = new DocumentID(2012, 2, 20, "DA41", "LLC", 
				"POL_MON3");
		Metadata metadata_2 = new Metadata(documentID_2, "ASDF Property", "TFING",
				"as76d6fh", 2, MongoManager.DEFAULT_PREALLOCATE_TIME);

		for (int i=0; i<6; i++)
			mongo.upsert(new Sample(metadata_2, 22, 31, 43+i, "6,896"), true);

		DocumentID documentID_3 = new DocumentID(2012, 2, 20, "DA41", "LLC", 
				"POL_MON3");
		Metadata metadata_3 = new Metadata(documentID_3, "ASDF Property", "TFING",
				"as76d6fh", 2, MongoManager.DEFAULT_PREALLOCATE_TIME);

		for (int i=0; i<6; i++)
			mongo.upsert(new Sample(metadata_3, 0, 0, i, "5"), true);
	}

	@Test
	public void testUpsertList() {
		DocumentID documentID_1 = new DocumentID(2012, 2, 25, "DA41", "LLC", 
				"POL_MON3");
		Metadata metadata_1 = new Metadata(documentID_1, "ASDF Property", "TFING",
				"as76d6fh", 2, MongoManager.DEFAULT_PREALLOCATE_TIME);

		List<Sample> list = new ArrayList<Sample>(10); 
		for (int i=0; i<10; i++) {
			list.add(new Sample(metadata_1, 23, 59, 33+i, "9,9"));
		}
		
		mongo.upsert(list, true);
	}
}
