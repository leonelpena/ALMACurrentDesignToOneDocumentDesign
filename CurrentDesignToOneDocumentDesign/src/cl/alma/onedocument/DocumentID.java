package cl.alma.onedocument;

import java.util.Calendar;
import java.util.GregorianCalendar;

/**
 * DocumentID class represent a MongoDB Object ID for the schema "One document
 * per component per day".
 * 
 * @author Leonel Peña <leo.dhpl@gmail.com>
 *
 */
public class DocumentID {

	//private Calendar _date;
	private String _antenna;
	private String _component;
	private String _monitorPoint;
	private String _id;
	private String _stringDate;
	
	private int _year;
	private int _month;
	private int _day;

	public static final String SEPARATOR = "/";  

	/**
	 * Instantiate a new DocumentID object
	 * @param date Date of the data.
	 * @param antenna Antenna name, i.e., 'DV10', 'CM12'
	 * @param component Component/subcomponent name, i.e., 'POL_MON4'
	 */
	//public DocumentID(Calendar date, String antenna, String component, 
	public DocumentID(int year, int month, int day, String antenna, String component,
			String monitorPoint) {

		this._year = year;
		this._month = month;
		this._day = day;
		this._antenna = antenna;
		this._component = component;
		this._monitorPoint = monitorPoint;

		//_stringDate = Integer.toString(date.get(Calendar.YEAR)) + 
			//	Integer.toString(date.get(Calendar.MONTH)) + 
				//Integer.toString(date.get(Calendar.DAY_OF_MONTH));
		//_date = new GregorianCalendar(year, month-1, day);
		_stringDate = Integer.toString(year) + "-" +
				Integer.toString(month) + "-" +
				Integer.toString(day);
		_id =  _stringDate.replace("-", "") + SEPARATOR + antenna + SEPARATOR + component +
				SEPARATOR + monitorPoint;
	}

	//public Calendar getDate() {
		//return _date;
	//}

	public String getStringDate() {
		return _stringDate;
	}

	public int getYear() {
		return _year;
	}

	public int getMonth() {
		return _month;
	}

	public int getDay() {
		return _day;
	}

	public String getAntenna() {
		return _antenna;
	}

	public String getComponent() {
		return _component;
	}
	
	public String getMonitorPoint() {
		return _monitorPoint;
	}

	@Override
	public String toString() {
		return _id;
	}

	public static void main(String[] argv) {
		//Calendar date = new GregorianCalendar(2012, 7, 23, 13, 34, 59);
		DocumentID doc = new DocumentID(2012, 7, 23, "CM01", "LLC", "POL_MON1");
		System.out.println("DocumetID: "+doc+"\n Date: "+doc.getStringDate());
	}
}
