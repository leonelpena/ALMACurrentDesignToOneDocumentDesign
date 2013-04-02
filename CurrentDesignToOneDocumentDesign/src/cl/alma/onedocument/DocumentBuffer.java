package cl.alma.onedocument;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * DocumentBuffer is a round buffer. This class is thread safe
 *     
 * @author Leonel Peña <leo.dhpl@gmail.com>
 *
 * @param <E>
 */
public class DocumentBuffer<E> {

	private static final int MAX_ELEMENTS = 10;
	//private ArrayList<E> buffer;
	private List<E> buffer;
	//private int size;
	//private int current_element;
	//private boolean first_round;
	private AtomicInteger size;
	private AtomicInteger current_element;
	private AtomicBoolean first_round;

	/**
	 * Instantiates a DocumentBuffer object with a fixed capacity
	 * 
	 * @param size
	 */
	public DocumentBuffer(int size) {
		this.size = new AtomicInteger(size);
		this.current_element = new AtomicInteger();
		//this.buffer = new ArrayList<E>(size+MAX_ELEMENTS);
		this.buffer = Collections.synchronizedList(
				new ArrayList<E>(size+MAX_ELEMENTS)
		);
		this.first_round = new AtomicBoolean(true);
	}

	/**
	 * Add or replace a element into the buffer
	 * 
	 * @param element
	 */
	public void set(E element) {
		if (element==null)
			throw new IllegalArgumentException("Null element"); 

		//if (first_round) {
		if (first_round.get()) {
			//buffer.add(current_element, element);
			buffer.add(current_element.get(), element);
		} else {
			//buffer.set(current_element, element);
			buffer.set(current_element.get(), element);
		}

		//current_element++;
		//if (current_element>=size) {
		if (current_element.incrementAndGet()>=size.get()) {
			current_element.set(0);
			//first_round = false;
			first_round.set(false);
		}
	}

	/**
	 * Returns true if this buffer contains the specified element and false 
	 * otherwise
	 * 
	 * @param element 
	 * @return
	 */
	public boolean contains(E element) {
		if (element==null)
			throw new IllegalArgumentException("Null element"); 

		return buffer.contains(element);
	}
}
