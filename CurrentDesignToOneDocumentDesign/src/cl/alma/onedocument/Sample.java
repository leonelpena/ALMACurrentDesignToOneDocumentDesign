package cl.alma.onedocument;

/**
 *  
 * @author Leonel Peña <leo.dhpl@gmail.com>
 *
 */
public class Sample {

	private Metadata metadata;
	private int hour;
	private int minute;
	private int second;
	private String value;

	/**
	 * Instantiate a new Sample
	 * @param metadata
	 * @param hour
	 * @param minute
	 * @param second
	 * @param value
	 */
	public Sample(Metadata metadata, int hour, int minute, int second,
			String value) {

		this.metadata = metadata;
		this.hour = hour;
		this.minute = minute;
		this.second = second;
		this.value = value;
	}

	public Metadata getMetadata() {
		return metadata;
	}

	public int getHour() {
		return hour;
	}

	public int getMinute() {
		return minute;
	}

	public int getSecond() {
		return second;
	}

	public String getValue() {
		return value;
	}
}
