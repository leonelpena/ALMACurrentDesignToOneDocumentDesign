package cl.alma.onedocumenttest;

import static org.junit.Assert.*;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import cl.alma.onedocument.DocumentID;
import cl.alma.onedocument.Metadata;
import cl.alma.onedocument.MongoManager;

public class MongoManagerTest {

	private static MongoManager mongo;

	@BeforeClass
	public static void setup() {
		try {
			mongo = new MongoManager("localhost", "JUnitTest", "MongoManagerTest");
		} catch (UnknownHostException e) {
			fail("Cannot connect to MongoDB instance");
		}
	}
	
	@AfterClass
	public static void clean() {
		/*try {
			mongo = new MongoManager("localhost", "JUnitTest", "MongoManagerTest");
		} catch (UnknownHostException e) {
			fail("Cannot connect to MongoDB instance");
		}*/
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
	public void testUpsertSample() {
		fail("Not yet implemented");
	}

	@Test
	public void testPreAllocate() {
		//fail("Not yet finished");
		
		// Sample time: 5 sec.
		DocumentID documentID = new DocumentID(2012, 9, 30, "DV04", "LLC", "POL_MON4");
		Metadata metadata = new Metadata(documentID, "ASDF Property", "TFING",
				"as76d6fh", 2, MongoManager.DEFAULT_PREALLOCATE_TIME);
		
		Calendar date = new GregorianCalendar(2012, 9, 30, 23, 2, 27);

		DBObject dbObject = mongo.preAllocate(metadata, date.getTime());
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
		
		boolean[] expected = new boolean[5];
		expected[0] = mongo.isDocumentCreated(doc1);
		expected[1] = mongo.isDocumentCreated(doc2);
		expected[2] = mongo.isDocumentCreated(doc3);
		expected[3] = mongo.isDocumentCreated(doc4);
		expected[4] = mongo.isDocumentCreated(doc5);

		boolean[] actual = new boolean[5];
		expected[0] = false;
		expected[1] = false;
		expected[2] = false;
		expected[3] = false; // because the document has not been added to mongo
		expected[4] = false; // because the document has not been added to mongo

		assertTrue(Arrays.equals(expected, actual));
	}

}
