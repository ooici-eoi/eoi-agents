/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package net.ooici.eoi.datasetagent;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.TimeZone;
import net.ooici.cdm.syntactic.Cdmdatatype;
import ucar.ma2.DataType;

/**
 * Provides utility methods which are shared between various Agent classes
 * 
 * @author cmueller
 */
public class AgentUtils {

	static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(AgentUtils.class);
	
	/**
	 * Retrieves the character data from the resource indicated by the given URL.  This method is used by
	 * Agents when making requests for data via customized URL query strings.
	 * 
	 * @param url
	 * @return
	 */
    public static String getDataString(String url) {
        StringBuilder sb = new StringBuilder();
        java.io.BufferedReader aomlReader = null;
        java.io.Reader rdr;
        try {
            if (url.startsWith("http://")) {
                java.net.HttpURLConnection conn = (java.net.HttpURLConnection) new java.net.URL(url).openConnection();
                rdr = new java.io.InputStreamReader(conn.getInputStream());
            } else {
                rdr = new java.io.FileReader(url);
            }

            aomlReader = new java.io.BufferedReader(rdr);
            String line;
            while ((line = aomlReader.readLine()) != null) {
                sb.append(line).append("\n");
            }
        } catch (java.net.MalformedURLException ex) {
        	LOGGER.error("Given URL cannot be parsed: '" + url + "'", ex);
        } catch (java.io.IOException ex) {
        	LOGGER.error("Could not get data from given url: '" + url + "'", ex);
        } finally {
            try {
                if (aomlReader != null) {
                    aomlReader.close();
                }
            } catch (java.io.IOException ex) {
            	LOGGER.warn("Could not close IO stream to given url: '" + url + "'", ex);
            }
        }
        return sb.toString();
    }


    public static Cdmdatatype.DataType getOoiDataType(DataType ucarDT) {
        Cdmdatatype.DataType ret = null;

        switch (ucarDT) {
            case BOOLEAN:
                ret = Cdmdatatype.DataType.BOOLEAN;
                break;
            case BYTE:
                ret = Cdmdatatype.DataType.BYTE;
                break;
            case SHORT:
                ret = Cdmdatatype.DataType.SHORT;
                break;
            case INT:
                ret = Cdmdatatype.DataType.INT;
                break;
            case LONG:
                ret = Cdmdatatype.DataType.LONG;
                break;
            case FLOAT:
                ret = Cdmdatatype.DataType.FLOAT;
                break;
            case DOUBLE:
                ret = Cdmdatatype.DataType.DOUBLE;
                break;
            case CHAR:
                ret = Cdmdatatype.DataType.CHAR;
                break;
            case STRING:
                ret = Cdmdatatype.DataType.STRING;
                break;
            case STRUCTURE:
                ret = Cdmdatatype.DataType.STRUCTURE;
                break;
            case SEQUENCE:
                ret = Cdmdatatype.DataType.SEQUENCE;
                break;
            case ENUM1:
            case ENUM2:
            case ENUM4:
                ret = Cdmdatatype.DataType.ENUM;
                break;
            case OPAQUE:
                ret = Cdmdatatype.DataType.OPAQUE;
                break;
            default:
                ret = Cdmdatatype.DataType.STRING;
        }
        return ret;
    }

    public static DataType getNcDataType(Cdmdatatype.DataType ooiDT) {
        DataType ret = null;

        switch (ooiDT) {
            case BOOLEAN:
                ret = DataType.BOOLEAN;
                break;
            case BYTE:
                ret = DataType.BYTE;
                break;
            case SHORT:
                ret = DataType.SHORT;
                break;
            case INT:
                ret = DataType.INT;
                break;
            case LONG:
                ret = DataType.LONG;
                break;
            case FLOAT:
                ret = DataType.FLOAT;
                break;
            case DOUBLE:
                ret = DataType.DOUBLE;
                break;
            case CHAR:
                ret = DataType.CHAR;
                break;
            case STRING:
                ret = DataType.STRING;
                break;
            case STRUCTURE:
                ret = DataType.STRUCTURE;
                break;
            case SEQUENCE:
                ret = DataType.SEQUENCE;
                break;
            case ENUM:
                ret = DataType.ENUM1;
                break;
            case OPAQUE:
                ret = DataType.OPAQUE;
                break;
            default:
                ret = DataType.STRING;
        }
        return ret;
    }


    /**
	 * SimpleDateFormat used for parsing incoming values mapped to START_TIME and END_TIME. This date format complies to the ISO 8601
	 * International Standard Representation of Dates and Times (http://www.w3.org/TR/NOTE-datetime)
	 */
	public static final DateFormat ISO8601_DATE_FORMAT;
    /**
	 * Static initializer for ISO8601_DATE_FORMAT field
	 */
	static {
		ISO8601_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		ISO8601_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
	}
}
