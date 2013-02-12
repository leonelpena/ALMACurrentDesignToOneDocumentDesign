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
	private int _sampleTime;
	private int _index;

	public Metadata(DocumentID documentID, String property, String location,
			String serialNumber, int sampleTime, int index) {

		this._documentID = documentID;
		this._property = property;
		this._location = location;
		this._serialNumber = serialNumber;
		this._sampleTime = sampleTime;
		this._index = index;
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