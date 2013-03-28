package cl.alma.onedocument;

import java.util.ArrayList;

/**
 * DocumentBuffer is a round buffer
 *     
 * @author Leonel Peña <leo.dhpl@gmail.com>
 *
 * @param <E>
 */
public class DocumentBuffer<E> {

	private static final int MAX_ELEMENTS = 10;
	private ArrayList<E> buffer;
	private int size;
	private int current_element;
	
	private boolean first_round;

	/**
	 * Instantiates a DocumentBuffer object with a fixed capacity
	 * 
	 * @param size
	 */
	public DocumentBuffer(int size) {
		this.size = size;
		this.current_element = 0;
		this.buffer = new ArrayList<E>(size+MAX_ELEMENTS);
		this.first_round = true;
	}

	/**
	 * Add or replace a element into the buffer
	 * 
	 * @param element
	 */
	public void set(E element) {
		if (element==null)
			throw new IllegalArgumentException("Null element"); 

		if (first_round) {
			buffer.add(current_element, element);
		} else {
			buffer.set(current_element, element);
		}

		current_element++;
		if (current_element>=size) {
			current_element = 0;
			first_round = false;
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
