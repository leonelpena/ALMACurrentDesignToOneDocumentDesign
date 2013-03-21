package cl.alma.onedocument;

/**
 * Metadata class represent the metadata object for the schema "One document
 * per component per day".
 * @author Leonel Peña <leo.dhpl@gmail.com>
 *
 */
public class Metadata {

	private DocumentID _documentID;
	private String _property;
	private String _location;
	private String _serialNumber;
	private int _index;
	private int _sampleTime;

	/**
	 * Instantiate a new Metadata object
	 * @param documentID
	 * @param property
	 * @param location
	 * @param serialNumber
	 * @param index
	 * @param sampleTime
	 */
	public Metadata(DocumentID documentID, String property, String location,
			String serialNumber, int index, int sampleTime) {

		if (documentID==null) {
			throw new IllegalArgumentException("DocumentID cannot be null");
		}

		if (sampleTime<=0) {
			throw new IllegalArgumentException("Sample time cannot be less or equal than zero");
		}

		this._documentID = documentID;
		this._property = property;
		this._location = location;
		this._serialNumber = serialNumber;
		this._index = index;
		this._sampleTime = sampleTime;
	}

	public DocumentID getDocumentID() {
		return _documentID;
	}

	public String getProperty() {
		return _property;
	}

	public String getLocation() {
		return _location;
	}

	public String getSerialNumber() {
		return _serialNumber;
	}

	public int getSampleTime() {
		return _sampleTime;
	}

	public int getIndex() {
		return _index;
	}

	
}