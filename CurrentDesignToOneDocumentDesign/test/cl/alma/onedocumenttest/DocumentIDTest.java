package cl.alma.onedocumenttest;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import cl.alma.onedocument.DocumentID;

public class DocumentIDTest {

	private DocumentID id;

	@Before
	public void setup() {
		id = new DocumentID(2012, 9, 23, "DV10", "LLC", "POL_MON4");
	}

	@Test
	public void testGetStringDate() {
		assertEquals("testGetStringDate", "2012-9-23", id.getStringDate());
	}

	@Test
	public void testToString() {
		assertEquals("toString", "2012923/DV10/LLC/POL_MON4", id.toString());
	}

}
