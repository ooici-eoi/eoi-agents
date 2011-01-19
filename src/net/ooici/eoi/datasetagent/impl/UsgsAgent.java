/*
 * File Name:  UsgsAgent.java
 * Created on: Dec 20, 2010
 */
package net.ooici.eoi.datasetagent.impl;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;

import net.ooici.eoi.datasetagent.DataSourceRequestKeys;
import net.ooici.eoi.datasetagent.obs.IObservationGroup;
import net.ooici.eoi.datasetagent.obs.IObservationGroup.DataType;
import net.ooici.eoi.datasetagent.NcdsFactory;
import net.ooici.eoi.datasetagent.obs.ObservationGroupImpl;
import net.ooici.eoi.datasetagent.VariableParams;
import net.ooici.eoi.datasetagent.AbstractAsciiAgent;
import net.ooici.eoi.datasetagent.IDatasetAgent;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;
import org.jdom.input.SAXBuilder;
import org.jdom.xpath.XPath;

import ucar.nc2.dataset.NetcdfDataset;

/**
 * TODO Add class comments
 * 
 * @author tlarocque
 * @version 1.0
 */
public class UsgsAgent extends AbstractAsciiAgent {
    
    /**
     * NOTE: this Object uses classes from org.jdom.* The JDOM library is included as a transitive dependency from
     * ooi-netcdf-full-4.2.4.jar. Should the JDOM jar be included explicitly??
     * 
     * TODO: check that each element selected by the xpath queries defined below are required per the WaterML1.1 spec. When these elements
     * are selected via xpath NULLs are not checked. If an element is not required, missing values will cause NPE
     */
    /** Static Fields */
    static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(SosAgent.class);
    private static final SimpleDateFormat valueSdf;
    private static final SimpleDateFormat inSdf;
    private static int currentGroupId = -1;
    

    /** XPATH queries */
    public static final String XPATH_ELEMENT_TIME_SERIES = ".//ns1:timeSeries";
    public static final String XPATH_ELEMENT_SITE_CODE = "./ns1:sourceInfo/ns1:siteCode";
    public static final String XPATH_ATTRIBUTE_AGENCY_CODE = "./ns1:sourceInfo/ns1:siteCode/@agencyCode";
    public static final String XPATH_ELEMENT_LATITUDE = "./ns1:sourceInfo/ns1:geoLocation/ns1:geogLocation/ns1:latitude"; /* NOTE: geogLocation is (1..*) */

    public static final String XPATH_ELEMENT_LONGITUDE = "./ns1:sourceInfo/ns1:geoLocation/ns1:geogLocation/ns1:longitude"; /* NOTE: geogLocation is (1..*) */

    public static final String XPATH_ELEMENT_VALUE = "./ns1:values/ns1:value";
    public static final String XPATH_ELEMENT_VARIABLE_CODE = "./ns1:variable/ns1:variableCode";
    public static final String XPATH_ELEMENT_VARIABLE_NAME = "./ns1:variable/ns1:variableName";
    public static final String XPATH_ELEMENT_VARIABLE_NaN_VALUE = "./ns1:variable/ns1:noDataValue";
    public static final String XPATH_ATTRIBUTE_QUALIFIERS = "./@qualifiers";
    public static final String XPATH_ATTRIBUTE_DATETIME = "./@dateTime";



    /** Maths */
    public static final double CONVERT_FT_TO_M = 0.3048;
    public static final double CONVERT_FT3_TO_M3 = Math.pow(CONVERT_FT_TO_M, 3);
    
    
    /** Static Initializer */
    static {
        valueSdf = new SimpleDateFormat("yyyy-MM-dd'T'HHmmss.sssZ");
        valueSdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        inSdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.sss");
        inSdf.setTimeZone(TimeZone.getTimeZone("UTC"));
    }
    
    /* (non-Javadoc)
     * @see net.ooici.agent.abstraction.IDatasetAgent#buildRequest(java.util.Map)
     */
    @Override
    public String buildRequest(Map<String, String[]> parameters) {
        LOGGER.debug("");
        LOGGER.info("Building SOS Request for context [" + parameters.toString().substring(0, 40) + "...]");
        
        StringBuilder result = new StringBuilder();


        String baseUrl = parameters.get(DataSourceRequestKeys.BASE_URL)[0];
        String sTimeString = parameters.get(DataSourceRequestKeys.START_TIME)[0];
        String eTimeString = parameters.get(DataSourceRequestKeys.END_TIME)[0];
        String properties[] = parameters.get(DataSourceRequestKeys.PROPERTY);
        String siteCodes[] = parameters.get(DataSourceRequestKeys.STATION_ID);


        /** TODO: null-check here */
        
        
        /** Configure the date-time parameter */
        Date sTime = null;
        Date eTime = null;
        try {
            sTime = DataSourceRequestKeys.ISO8601_FORMAT.parse(sTimeString);
        } catch (ParseException e) {
            throw new IllegalArgumentException("Could not convert DATE string for context key " + DataSourceRequestKeys.START_TIME + "Unparsable value = " + sTimeString, e);
        }
        try {
            eTime = DataSourceRequestKeys.ISO8601_FORMAT.parse(eTimeString);
        } catch (ParseException e) {
            throw new IllegalArgumentException("Could not convert DATE string for context key " + DataSourceRequestKeys.END_TIME + "Unparsable value = " + eTimeString, e);
        }
        DateFormat usgsUrlSdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
        usgsUrlSdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        sTimeString = usgsUrlSdf.format(sTime);
        eTimeString = usgsUrlSdf.format(eTime);



        /** Build the propertiesString*/
        StringBuilder propertiesString = new StringBuilder();
        for (String property : properties) {
            if (null != property) {
                propertiesString.append(property.trim()).append(",");
            }
        }
        if (propertiesString.length() > 0) {
            propertiesString.deleteCharAt(propertiesString.length() - 1);
        }
        
        
        
        /** Build the list of sites (siteCSV)*/
        StringBuilder siteCSV = new StringBuilder();
        for (String siteCode : siteCodes) {
            if (null != siteCode) {
                siteCSV.append(siteCode.trim()).append(",");
            }
        }
        if (siteCSV.length() > 0) {
            siteCSV.deleteCharAt(siteCSV.length() - 1);
        }
        
        
        
        /** Build the query URL */
        result.append(baseUrl);
        result.append("&sites=").append(siteCSV);
        result.append("&parameterCd=").append(propertiesString);
        result.append("&startDT=").append(sTimeString);
        result.append("&endDT=").append(eTimeString);

        

        LOGGER.debug("... built request: [" + result + "]");
        return result.toString();
    }

    /* (non-Javadoc)
     * @see net.ooici.agent.abstraction.AbstractAsciiAgent#parseObss(java.lang.String)
     */
    @Override
    protected List<IObservationGroup> parseObs(String asciiData) {
        LOGGER.debug("");
        LOGGER.info("Parsing observations from data [" + asciiData.substring(0, 40) + "...]");
        
        IObservationGroup obs = null;
        StringReader srdr = new StringReader(asciiData);
        try {
            obs = parseObservations(srdr);
        } finally {
            if (srdr != null) {
                srdr.close();
            }
        }

        
        List<IObservationGroup> obsList = new ArrayList<IObservationGroup>();
        obsList.add(obs);
        return obsList;
    }
    
    /**
     * Parses the String data from the given reader into a list of observation groups.
     * <em>Note:</em><br />
     * The given reader is guaranteed to return from this method in a <i>closed</i> state.
     * @param rdr
     * @return a List of IObservationGroup objects if observations are parsed, otherwise this list will be empty
     */
    public static IObservationGroup parseObservations(Reader rdr) {
        /* TODO: Fix exception handling in this method, it is too generic; try/catch blocks should be as confined as possible */
        IObservationGroup obs = null;
        SAXBuilder builder = new SAXBuilder();
        Document doc = null;
        String datetime = "[no date information]"; /* datetime defined here (outside try) for error reporting */

        try {
            doc =  builder.build(rdr);
            
            /** Grab Global Attributes (to be copied into each observation group */
            Namespace ns1 = Namespace.getNamespace("ns1", "http://www.cuahsi.org/waterML/1.1/");
//          Namespace ns2 = Namespace.getNamespace("ns2", "http://waterservices.usgs.gov/WaterML-1.1.xsd");
//          Namespace xsi = Namespace.getNamespace("xsi", "http://www.w3.org/2001/XMLSchema");
            
            
            Element root = doc.getRootElement();
            Element queryInfo = root.getChild("queryInfo", ns1);
            Map<String, String> globalAttributes = new HashMap<String, String>();
            
            
            /* Extract the Global Attributes */
            /* title */
            String queryUrl = xpathSafeSelectValue(queryInfo, ".//ns2:queryURL", null);
            globalAttributes.put("title", "USGS rivers data timeseries.  Requested from \"" + queryUrl + "\"");
            
            /* history */
            globalAttributes.put("history", "Converted from WaterML1.1 to OOI CDM compliant NC by " + UsgsAgent.class.getName());
            
            /* references */
            globalAttributes.put("references", new StringBuilder().append("[")
                                                                  .append("http://waterservices.usgs.gov/")
                                                                  .append("]")
                                                                  .toString());
            
            /* utc_start_time */
            Date tempDate = new Date(0);
            String beginDate = xpathSafeSelectValue(queryInfo, ".//ns2:timeParam/ns2:beginDateTime", null);
            
            tempDate = inSdf.parse(beginDate);
            String beginDateISO = outSdf.format(tempDate);
            globalAttributes.put("utc_start_time", beginDateISO);
            
            
            /* utc_end_time */
            tempDate = new Date(0);
            String endDate = xpathSafeSelectValue(queryInfo, ".//ns2:timeParam/ns2:endDateTime", null);
            
            
            tempDate = inSdf.parse(endDate);
            String endDateISO = outSdf.format(tempDate);
            globalAttributes.put("utc_end_time", endDateISO);
            
            
            /* conventions */
            globalAttributes.put("Conventions", "CF-1.5");
            
            

            /** Get a list of provided time series */
            List<?> timeseriesList = XPath.selectNodes(doc, XPATH_ELEMENT_TIME_SERIES);

            

            /** Build an observation group for each unique sitecode */
            Object nextTimeseries = null;
            Iterator<?> iterTimeseries = timeseriesList.iterator();
            while (iterTimeseries.hasNext()) {
                /* Grab the next element */
                nextTimeseries = iterTimeseries.next();
                if (null == nextTimeseries) {
                    continue;
                }


                /** Grab data for the current site */
                String stnId = ((Element) XPath.selectSingleNode(nextTimeseries, XPATH_ELEMENT_SITE_CODE)).getTextTrim();
                String latitude = ((Element) XPath.selectSingleNode(nextTimeseries, XPATH_ELEMENT_LATITUDE)).getTextTrim();
                String longitude = ((Element) XPath.selectSingleNode(nextTimeseries, XPATH_ELEMENT_LONGITUDE)).getTextTrim();
                String noDataString = ((Element) XPath.selectSingleNode(nextTimeseries, XPATH_ELEMENT_VARIABLE_NaN_VALUE)).getTextTrim();

                float lat = Float.parseFloat(latitude);
                float lon = Float.parseFloat(longitude);
                // float noDataValue = Float.parseFloat(noDataString);


                /* Check to see if the observation group already exists */
                if (obs == null) {
                    /* Create a new observation group if one does not currently exist */
                    obs = new ObservationGroupImpl(getNextGroupId(), stnId, lat, lon);
                }



                /** Grab variable data */
                String variableCode = ((Element) XPath.selectSingleNode(nextTimeseries, XPATH_ELEMENT_VARIABLE_CODE)).getTextTrim();
                // String variableUnits = getUnitsForVariableCode(variableCode);
                // String variableName = getStdNameForVariableCode(variableCode);



                /** Add each timeseries value (observation) to the observation group */
                /* Get a list of each observation */
                List<?> observationList = XPath.selectNodes(nextTimeseries, XPATH_ELEMENT_VALUE);


                /* Add an observation for each "value" parsed */
                Object next = null;
                Iterator<?> iter = observationList.iterator();
                while (iter.hasNext()) {
                    /* Grab the next element */
                    next = iter.next();
                    if (null == next) {
                        continue;
                    }

                    /* Grab observation data */
                    // String qualifier = ((org.jdom.Attribute) XPath.selectSingleNode(next, XPATH_ATTRIBUTE_QUALIFIERS)).getValue();
                    datetime = ((org.jdom.Attribute) XPath.selectSingleNode(next, XPATH_ATTRIBUTE_DATETIME)).getValue();
                    String value = ((Element) next).getTextTrim();
                    datetime = datetime.replaceAll("\\:", "");


                    /* Convert observation data */
                    int time = 0;
                    float data = 0;
                    float dpth = 0;
                    VariableParams name = null;

                    time = (int) (valueSdf.parse(datetime).getTime() * 0.001);
                    // data = Float.parseFloat(value);
                    name = getDataNameForVariableCode(variableCode);
                    /* Only convert data if we are dealing with Steamflow) */
                    if (name == VariableParams.RIVER_STREAMFLOW) {
                        data = (noDataString.equals(value)) ? (Float.NaN) : (float) (Double.parseDouble(value) * CONVERT_FT3_TO_M3); /* convert from (f3 s-1) --> (m3 s-1) */
                    } else {
                        data = (noDataString.equals(value)) ? (Float.NaN) : (float) (Double.parseDouble(value));
                    }
                    dpth = 0;


                    /* Add the observation data */
                    obs.addObservation(time, dpth, data, new VariableParams(name, DataType.FLOAT));

                    /* TODO: Add the data observation qualifier */
                    // og.addObservation(time, dpth, qualifier, new VariableParams(name, DataType.FLOAT));

                }
                
                
                /** Grab attributes */
                Map<String, String> tsAttributes = new TreeMap<String, String>();
                

                /* Extract timeseries-specific attributes */
                String sitename = xpathSafeSelectValue(nextTimeseries, "//ns1:siteName",  "[n/a]");
                String network = xpathSafeSelectValue(nextTimeseries, "//ns1:siteCode/@network",  "[n/a]");
                String agency = xpathSafeSelectValue(nextTimeseries, "//ns1:siteCode/@agencyCode",  "[n/a]");
                String sCode = xpathSafeSelectValue(nextTimeseries, "//ns1:siteCode",  "[n/a]");
                tsAttributes.put("institution", new StringBuilder().append(sitename)
                                                                   .append(" (network:").append(network)
                                                                   .append("; agencyCode:").append(agency)
                                                                   .append("; siteCode:").append(sCode)
                                                                   .append(";)").toString());
                
                String method = xpathSafeSelectValue(nextTimeseries, ".//ns1:values//ns1:methodDescription",  "[n/a]");
                tsAttributes.put("source", method);
                
                
                
                /** Add global and timeseries attributes */
                obs.addAttributes(globalAttributes);
                obs.addAttributes(tsAttributes);
                

            }

        } catch (JDOMException ex) {
            LOGGER.error("Error while parsing xml from the given reader", ex);
        } catch (IOException ex) {
            LOGGER.error("General IO exception.  Please see stack-trace", ex);
        } catch (ParseException ex) {
            LOGGER.error("Could not parse date information from XML result for: " + datetime, ex);
        }

        return obs;
    }
    
    private static String xpathSafeSelectValue(Object context, String path, String defaultValue, Namespace... namespaces) {
        
        Object result = null;
        try {
            XPath xp = XPath.newInstance(path);
            if (null != namespaces) {
                for (Namespace namespace : namespaces) {
                    xp.addNamespace(namespace);
                }
            }
            result = xp.selectSingleNode(context);
        } catch (JDOMException ex) {
            LOGGER.debug("Could not select node via XPath query: \"" + path + "\"", ex);
        }
        
        
        return xpathNodeValue(result, defaultValue);
    }
    
    /**
     * <b>Note:</b><br />
     * This method does not support all types of returns from XPath.selectSingleNode().
     * It will currently support:<br />
     *      org.jdom.Attribute<br />
     *      org.jdom.Element<br />
     * @param node
     * @param defaultValue
     * @return
     */
    private static String xpathNodeValue(Object node, String defaultValue) {
        /* Overwrite defaultValue if value can be retrieved from node */
        if (node instanceof org.jdom.Attribute) {
            defaultValue = ((org.jdom.Attribute)node).getValue();
        } else if (node instanceof org.jdom.Element) {
            defaultValue = ((org.jdom.Element)node).getText();
        }
        
        
        return defaultValue;
    }
    
    protected static int getCurrentGroupId() {
        return currentGroupId;
    }

    protected static int getNextGroupId() {
        return ++currentGroupId;
    }
    

    protected static VariableParams getDataNameForVariableCode(String variableCode) {
        VariableParams result = null;

        if ("00010".equals(variableCode)) {
            result = VariableParams.WATER_TEMPERATURE;
        } else if ("00060".equals(variableCode)) {
            result = VariableParams.RIVER_STREAMFLOW;
        } else {
            throw new IllegalArgumentException("Given variable code is not known: " + variableCode);
        }

        return result;
    }

    /* (non-Javadoc)
     * @see net.ooici.agent.abstraction.AbstractAsciiAgent#obs2Ncds(java.util.List)
     */
    @Override
    protected NetcdfDataset obs2Ncds(List<IObservationGroup> observations) {
        LOGGER.debug("");
        LOGGER.info("Creating NC Dataset as a 'station' feature type...");
        
        NetcdfDataset ncds = null;
        if (!observations.isEmpty()) {
            ncds = NcdsFactory.buildStation(observations.get(0));
        } else {
            LOGGER.warn("Unusable argument:  Given observations List is empty");
        }
        
        if (observations.size() > 1) {
            LOGGER.warn("Unexpected loss of data: Given List of observations contains more than 1 group which will not be used to produce NCDS output.  Total Observation Groups: " + observations.size());
        }
        
        return ncds;
    }

    
    
    /*****************************************************************************************************************/
    /* Testing                                                                                                       */
    /*****************************************************************************************************************/

    
    /* TODO: */
    public static void main(String[] args) {
        IDatasetAgent agent = new UsgsAgent();
        NetcdfDataset dataset = agent.doUpdate(TEST_CONTEXT_STATION_1);
//        NetcdfDataset dataset = agent.doUpdate(TEST_CONTEXT_STATION_2);
        if (! new File(outDir).exists()) {
            new File(outDir).mkdirs();
        }
        String outName = "USGS_" + ++outCount + ".nc";
        try {
            LOGGER.info("Writing NC output to [" + outDir + outName + "]...");
            ucar.nc2.FileWriter.writeToFile(dataset, outDir + outName);
        } catch (IOException ex) {
            LOGGER.warn("Could not write NC to file: " + outDir + outName, ex);
        }
        
    }
    
    private static Map<String, String[]> TEST_CONTEXT_STATION_1 = new HashMap<String, String[]>();
    private static Map<String, String[]> TEST_CONTEXT_STATION_2 = new HashMap<String, String[]>();
    private static String outDir = "output/usgs/";
    private static int outCount = 0;
    
    /* TODO: */
    static {
        TEST_CONTEXT_STATION_1.put("id",         new String[] {"USGS"});
        TEST_CONTEXT_STATION_1.put("base_url",   new String[] {"http://waterservices.usgs.gov/nwis/iv?"});
        TEST_CONTEXT_STATION_1.put("start_time", new String[] {"2010-10-10T00:00:00Z"});
        TEST_CONTEXT_STATION_1.put("end_time",   new String[] {"2010-10-12T00:00:00Z"});
        TEST_CONTEXT_STATION_1.put("property",   new String[] {"00010"});
        TEST_CONTEXT_STATION_1.put("stationId",  new String[] {"01463500"});
        
        TEST_CONTEXT_STATION_2.put("id",         new String[] {"USGS"});
        TEST_CONTEXT_STATION_2.put("base_url",   new String[] {"http://waterservices.usgs.gov/nwis/iv?"});
        TEST_CONTEXT_STATION_2.put("start_time", new String[] {"2010-10-10T00:00:00Z"});
        TEST_CONTEXT_STATION_2.put("end_time",   new String[] {"2010-10-12T00:00:00Z"});
        TEST_CONTEXT_STATION_2.put("property",   new String[] {"00060"});
        TEST_CONTEXT_STATION_2.put("stationId",  new String[] {"01463500"});
        
        
    }
}