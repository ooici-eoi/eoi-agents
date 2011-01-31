package net.ooici.eoi.datasetagent;


import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.TimeZone;


/**
 * Class which defines various constants for Data Source requests.  These constants are used by individual Dataset Agents when
 * building query Strings, such as, through their buildUrl() methods
 *  
 * @author tlarocque
 */
public final class DataSourceRequestKeys {

	/**
	 * Privatized constructor to prevent instantiation
	 */
	private DataSourceRequestKeys() {
		/* NO-OP */
	}


	/**
	 * SimpleDateFormat used for parsing incoming values mapped to START_TIME and END_TIME. This date format complies to the ISO 8601
	 * International Standard Representation of Dates and Times (http://www.w3.org/TR/NOTE-datetime)
	 */
    @Deprecated
	public static final DateFormat ISO8601_FORMAT;
	/**
	 * The name of a given data source. Source name may provide one or more datasets, but this mapping provides a single value for
	 * 'source_name'
	 */
	public static final String SOURCE_TYPE = "source_type";
	/**
	 * The starting time for the datasource
	 */
	public static final String START_TIME = "start_time";
	/**
	 * The ending time for the datasource
	 */
	public static final String END_TIME = "end_time";
	/**
	 * Indicates a variable/observation name (i.e. salinity); may be mapped to one or more values
	 */
	public static final String PROPERTY = "property";
	/**
	 * The identifier(s) of the station(s) of interest; may be mapped to one ore more values
	 */
	public static final String STATION_ID = "stationId";
	/**
	 * Request "type" for services that can obtain many kinds of data. (Ex: AOML service may provide XBT or CTD data)
	 */
	public static final String TYPE = "type";
	/**
	 * Indicates the northern-most latitude bound
	 */
	public static final String TOP = "top";
	/**
	 * Indicates the southern-most latitude bound
	 */
	public static final String BOTTOM = "bottom";
	/**
	 * Indicates the western-most longitude bound
	 */
	public static final String LEFT = "left";
	/**
	 * Indicates the eastern-most longitude bound
	 */
	public static final String RIGHT = "right";
	/**
	 * The URL of the service being called
	 */
	public static final String BASE_URL = "base_url";
	/**
	 * The fully qualified URL of the last retrieval of a dataset
	 */
	public static final String DATASET_URL = "dataset_url";



	/**
	 * Static initializer for ISO8601_FORMAT field
	 */
	static {
		ISO8601_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		ISO8601_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
	}
}
