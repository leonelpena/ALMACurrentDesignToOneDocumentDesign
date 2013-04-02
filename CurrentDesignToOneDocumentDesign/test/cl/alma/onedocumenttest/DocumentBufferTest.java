package cl.alma.onedocumenttest;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import cl.alma.onedocument.DocumentBuffer;

public class DocumentBufferTest {

	@Test
	public void test() {

		DocumentBuffer<String> buffer = new DocumentBuffer<String>(10);

		for (int i=0; i<13; i++) {
			buffer.set("hi "+i);
		}
		buffer.set("testing");

		boolean[] actuals = new boolean[10];
		for (int i=0; i<10; i++) {
			actuals[i] = buffer.contains("hi "+i);
		}

		actuals[3] = buffer.contains("testing");
		
		boolean[] expecteds = new boolean[10];
		expecteds[0] = false;
		expecteds[1] = false;
		expecteds[2] = false;
		expecteds[3] = true;
		expecteds[4] = true;
		expecteds[5] = true;
		expecteds[6] = true;
		expecteds[7] = true;
		expecteds[8] = true;
		expecteds[9] = true;

		assertTrue(Arrays.equals(expecteds, actuals));
	}
}
