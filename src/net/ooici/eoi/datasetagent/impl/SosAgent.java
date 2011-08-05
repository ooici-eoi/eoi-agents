/*
 * File Name:  SosAgent.java
 * Created on: Dec 17, 2010
 */
package net.ooici.eoi.datasetagent.impl;

import ion.core.IonException;
import ion.core.utils.GPBWrapper;
import ion.core.utils.IonUtils;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import net.ooici.Pair;
import net.ooici.eoi.datasetagent.obs.IObservationGroup;
import net.ooici.eoi.datasetagent.obs.ObservationGroupImpl;
import net.ooici.eoi.netcdf.VariableParams;
import net.ooici.eoi.datasetagent.AbstractAsciiAgent;
import net.ooici.eoi.datasetagent.AgentFactory;
import net.ooici.eoi.datasetagent.AgentUtils;
import net.ooici.services.dm.IngestionService.DataAcquisitionCompleteMessage.StatusCode;
import net.ooici.services.sa.DataSource.EoiDataContextMessage;
import ucar.nc2.dataset.NetcdfDataset;

/**
 * The SosAgent class is designed to fulfill updates for datasets which originate from SOS services. Ensure the update context (
 * {@link EoiDataContextMessage}) to be passed to {@link #doUpdate(EoiDataContextMessage, HashMap)} has been constructed for SOS agents by
 * checking the result of {@link EoiDataContextMessage#getSourceType()}
 *
 * @author cmueller
 * @author tlarocque
 * @version 1.0
 * @see {@link EoiDataContextMessage#getSourceType()}
 * @see {@link AgentFactory#getDatasetAgent(net.ooici.services.sa.DataSource.SourceType)}
 */
public class SosAgent extends AbstractAsciiAgent {

    /** Static Fields */
    static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(SosAgent.class);
    private static long beginTime = Long.MAX_VALUE;
    private static long endTime = Long.MIN_VALUE;
    protected static final SimpleDateFormat sdf;
    private String data_url;
    private String obsName;

    static {
        sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    /**
     * Constructs a URL from the given data <code>context</code> by appending necessary SOS-specific query string parameters to the base URL
     * returned by <code>context.getBaseUrl()</code>. This URL may subsequently be passed through {@link #acquireData(String)} to procure
     * updated data according to the <code>context</code> given here.
     * 
     * @param context
     *            the current or required state of an SOS dataset providing context for building data requests to fulfill dataset updates
     * @return A dataset update request URL built from the given <code>context</code> against an SOS service.
     */
    @Override
    public String buildRequest() {
        log.debug("");
        log.info("Building SOS Request for the given context:\n{\n" + context.toString() + "}\n");

        StringBuilder result = new StringBuilder();

        log.debug("Localizing context...");
        String baseUrl = context.getBaseUrl();
        String north = String.valueOf(context.getRequestBoundsNorth());
        String south = String.valueOf(context.getRequestBoundsSouth());
        String west = String.valueOf(context.getRequestBoundsWest());
        String east = String.valueOf(context.getRequestBoundsEast());
        String property = context.getPropertyList().get(0);
        obsName = property;
        String stnId = context.getStationIdList().get(0);


        /** TODO: null-check here */
        /** Configure the date-time parameter (if avail) */
        log.debug("Configuring date-time");
        String eventTime = null;

        if (context.hasStartDatetimeMillis() && context.hasEndDatetimeMillis()) {
            SimpleDateFormat sosUrlSdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
            sosUrlSdf.setTimeZone(TimeZone.getTimeZone("UTC"));

            eventTime = new StringBuilder(sosUrlSdf.format(new Date(context.getStartDatetimeMillis()))).append('/').append(sosUrlSdf.format(new Date(context.getEndDatetimeMillis()))).toString();
        }

//        if (null != sTimeString && null != eTimeString && !sTimeString.isEmpty() && !eTimeString.isEmpty()) {
//            Date sTime = null;
//            Date eTime = null;
//            try {
//                sTime = AgentUtils.ISO8601_DATE_FORMAT.parse(sTimeString);
//            } catch (ParseException e) {
//                log.error("Error parsing start time - the start time will not be specified", e);
////                throw new IllegalArgumentException("Could not convert DATE string for context start_time:: Unparsable value = " + sTimeString, e);
//            }
//            try {
//                eTime = AgentUtils.ISO8601_DATE_FORMAT.parse(eTimeString);
//            } catch (ParseException e) {
//                log.error("Error parsing end time - the end time will not be specified", e);
////                throw new IllegalArgumentException("Could not convert DATE string for context end_time:: Unparsable value = " + eTimeString, e);
//            }
//            DateFormat sosUrlSdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
//            sosUrlSdf.setTimeZone(TimeZone.getTimeZone("UTC"));
//            sTimeString = sosUrlSdf.format(sTime);
//            eTimeString = sosUrlSdf.format(eTime);
//
//            eventTime = new StringBuilder(sTimeString).append('/').append(eTimeString).toString();
//        }



        /** Configure the BBOX parameter (if avail) */
        String bbox = null;
        if (null != north && null != south && null != west && null != east) {
            bbox = new StringBuilder(west).append(',').append(south).append('.').append(east).append(',').append(north).append(',').toString();
        }



        /** Build the query URL */
        result.append(baseUrl);
        result.append("request=").append("GetObservation");
        result.append("&service=").append("SOS");
        result.append("&observedproperty=").append(property);
        if (null != stnId) {
            result.append("&offering=urn:ioos:station:wmo:").append(stnId);
        } else if (null != bbox) {
            result.append("&offering=urn:ioos:network:noaa.nws.ndbc:all");
            result.append("&featureofInterest=BBOX:").append(bbox);
        } else {
            throw new IllegalArgumentException("Cannot make a request without either a station ID or bounding box");
        }
        if (null != eventTime) { /* omitting time retrieves latest data */
            result.append("&eventtime=").append(eventTime);
        }
        result.append("&responseformat=").append("text/csv");



        data_url = result.toString();
        log.debug("... built request: [" + data_url + "]");
        return data_url;
    }

    /**
     * Parses the given SOS <code>String</code> data (CSV) as a list of <code>IObservationGroup</code> objects
     * 
     * @param asciiData
     *            CSV data passed to this method from {@link #acquireData(String)}
     * 
     * @return a list of <code>IObservationGroup</code> objects representing the observations parsed from the given <code>asciiData</code>
     */
    @Override
    protected List<IObservationGroup> parseObs(String asciiData) {
        if (log.isDebugEnabled()) {
            log.debug("Parsing observations from data [{}...]", asciiData.substring(0, 40));
        }

        List<IObservationGroup> obsList = null;
        StringReader srdr = new StringReader(asciiData);
        try {
            obsList = parseObservations(srdr);
        } finally {
            if (srdr != null) {
                srdr.close();
            }
        }

        return obsList;
    }

    /**
     * Parses the String data from the given reader into a list of observation groups.<br />
     * <br />
     * <b>Note:</b><br />
     * The given reader is guaranteed to return from this method in a <i>closed</i> state.
     * 
     * @param rdr
     *            a <code>Reader</code> object linked to a stream of SOS ascii data
     * @return a List of IObservationGroup objects if observations are parsed, otherwise this list will be empty
     */
    public List<IObservationGroup> parseObservations(Reader rdr) {
        List<IObservationGroup> obsList = new ArrayList<IObservationGroup>();
        IObservationGroup obs = null;
        BufferedReader csvReader = null;
        try {
            csvReader = new BufferedReader(rdr);

            String line = csvReader.readLine();//headerline
            if (line == null) {
                /* Even when no dataVar is available, a header is returned.
                Therefore, this should never happen unless the url is improperly formed which would result in an HTTP error of some sort. */
                log.warn("Unusable argument: Given reader contains no String data");
                return null;
            }

            String[] tokens;
            tokens = line.split(",");

            /* Sort out depth */
            int depthIndex = -1;// default is no depth field --> gets set to 0
            int startIndex = 5;// 5 if no depth, otherwise should be depthIndex + 1;
            for (int i = 0; i < tokens.length; i++) {
                if (tokens[i].equalsIgnoreCase("\"depth (m)\"")) {
                    /* We have depth - */
                    depthIndex = i;
                    startIndex = i + 1;
                    break;
                }
            }

            List<Pair<Integer, VariableParams>> dataCols = new ArrayList<Pair<Integer, VariableParams>>();
            VariableParams params;
            String name;
//            String varName = "";
            for (int i = startIndex; i < tokens.length; i++) {
                /* Remove any quoting (occurs for all headers with units) */
                name = tokens[i].replaceAll("\"", "");
                /* Store the original name */
                String oname = name;
                /* Chop off the units (if present) and any leading/trailing whitespace */
                int ei = name.indexOf("(");
                name = name.substring(0, ei > 0 ? ei : name.length() - 1);
                name = name.trim();
                try {
                    params = VariableParams.StandardVariable.parseStandardName(name).getVariableParams();
                } catch (IllegalArgumentException ex) {
                    /* Ensure the name is a valid Netcdf3 name */
                    name = ucar.nc2.iosp.netcdf3.N3iosp.createValidNetcdf3ObjectName(name);

                    log.error("Data header \"{}\" unrecognized - adding generic \"{}\"", oname, name);
                    params = new VariableParams(null, name, oname, "1");
                }
                dataCols.add(new Pair<Integer, VariableParams>(i, new VariableParams(params, IObservationGroup.DataType.FLOAT)));
//                varName += (varName.isEmpty()) ? params.getStandardName() : "-" + params.getStandardName();

//                if (tokens[i].toLowerCase().contains("sea_water_temperature")) {
////                    dataCols.add(new Pair<Integer, VariableParams>(i, VariableParams.SEA_WATER_TEMPERATURE));
//                    dataCols.add(new Pair<Integer, VariableParams>(i, new VariableParams(VariableParams.StandardVariable.SEA_WATER_TEMPERATURE, IObservationGroup.DataType.FLOAT)));
//                    varName += (varName.isEmpty()) ? VariableParams.VariableParams.SEA_WATER_TEMPERATURE.getStandardName() : "-" + VariableParams.SEA_WATER_TEMPERATURE.getStandardName();
//                } else if (tokens[i].toLowerCase().contains("sea_water_salinity")) {
////                    dataCols.add(new Pair<Integer, VariableParams>(i, VariableParams.SEA_WATER_SALINITY));
//                    dataCols.add(new Pair<Integer, VariableParams>(i, new VariableParams(VariableParams.SEA_WATER_SALINITY, IObservationGroup.DataType.FLOAT)));
//                    varName = (varName.isEmpty()) ? VariableParams.SEA_WATER_SALINITY.getStandardName() : "-" + VariableParams.SEA_WATER_SALINITY.getStandardName();
//                } else if (tokens[i].toLowerCase().contains("air_temperature")) {
////                    dataCols.add(new Pair<Integer, VariableParams>(i, VariableParams.SEA_WATER_SALINITY));
//                    dataCols.add(new Pair<Integer, VariableParams>(i, new VariableParams(VariableParams.AIR_TEMPERATURE, IObservationGroup.DataType.FLOAT)));
//                    varName = (varName.isEmpty()) ? VariableParams.AIR_TEMPERATURE.getStandardName() : "-" + VariableParams.AIR_TEMPERATURE.getStandardName();
//                } else if (tokens[i].toLowerCase().contains("air_pressure_at_sea_level")) {
//                    dataCols.add(new Pair<Integer, VariableParams>(i, new VariableParams(VariableParams.AIR_PRESSURE_AT_SEA_LEVEL, IObservationGroup.DataType.FLOAT)));
//                } else if (tokens[i].toLowerCase().contains("waves")) {
////                    dataCols.add(new Pair<Integer, VariableParams>(i, new VariableParams(VariableParams.WAVES, IObservationGroup.DataType.FLOAT)));
//
//                    log.debug("waves");
//                } else if (tokens[i].toLowerCase().contains("winds")) {
////                    dataCols.add(new Pair<Integer, VariableParams>(i, new VariableParams(VariableParams.WINDS, IObservationGroup.DataType.FLOAT)));
//
//                    log.debug("winds");
//                } else {
//                    log.debug("Unknown token: " + tokens[i]);
//                }
            }

            
            String stnId = "";
//            String snsId = "";
            int time = 0;
            float lat = -9999, tla = -99991, lon = -9999, tlo = -99991, depth = -9999/*, td = -99991*/;
            int obsId = 0;
            
            line = csvReader.readLine();
            if (null == line) {
                log.warn("No data retrieved for this request!!");
                return new ArrayList<IObservationGroup>();
            }
            
            
            
            while (line != null) {
                tokens = line.split(",");
                tla = Float.valueOf(tokens[2]);
                tlo = Float.valueOf(tokens[3]);
                if (obs == null || (lat != tla) || (lon != tlo)/*|| !stnId.equals(tokens[0]) || !snsId.equals(tokens[1]) || (lat != (tla = Float.valueOf(tokens[2]))) || (lon != (tlo = Float.valueOf(tokens[3])))*/) {
                    if (obs != null) {
                        obsList.add(obs);
                    }
                    /* New group of observations */
                    stnId = tokens[0];
//                    snsId = tokens[1];
                    lat = tla;
                    lon = tlo;
//                    og = new ObservationGroupImpl(obsId++, stnId, snsId, lat, lon);
                    obs = new ObservationGroupImpl(obsId++, stnId, lat, lon);
                }

                /* Add the next time & depth */
                try {
                    long longTime = sdf.parse(tokens[4]).getTime();
                    beginTime = Math.min(beginTime, longTime);
                    endTime = Math.max(endTime, longTime);
                    time = (int) (longTime * 0.001);
                } catch (java.text.ParseException ex) {
                    time = 0;
                } catch (NumberFormatException ex) {
                    time = 0;
                } catch (ArrayIndexOutOfBoundsException ex) {
                    time = 0;
                }
                if (depthIndex >= 0) {
                    try {
                        depth = Float.valueOf(tokens[depthIndex]);
                    } catch (NumberFormatException ex) {
                        /* Can have missing depth (I believe when the instrument does not report it's depth or is an older record) - assign to depth of 0 when this happens. */
                        depth = 0.0f;
                    } catch (ArrayIndexOutOfBoundsException ex) {
                        depth = 0.0f;
                    }
                } else {
                    depth = 0.0f;
                }
                Pair<Integer, VariableParams> dc;
                float val;
                for (int di = 0; di < dataCols.size(); di++) {
                    dc = dataCols.get(di);
                    try {
                        val = Float.valueOf(tokens[dc.getKey()]);
                    } catch (NumberFormatException ex) {
                        /* Data not a value... */
                        val = Float.NaN;
                    } catch (ArrayIndexOutOfBoundsException ex) {
                        /* If data is missing. */
                        val = Float.NaN;
                    }
                    obs.addObservation(time, depth, val, dc.getValue());
                }
                
                
                /** Get the next line */
                line = csvReader.readLine();
            }

            
            /* Add the last obs group */
            if (obs != null) {
                obsList.add(obs);
            }

            /** Add Global Attributes to each observation group */
            Map<String, String> globalAttributes = new HashMap<String, String>();

            /* title */
//        String queryUrl = "http://sdf.ndbc.noaa.gov/sos/";
//        globalAttributes.put("title", "NDBC Sensor Observation Service data from \"" + queryUrl + "\"");
//            String title = "SOS (" + stnId + ") " + varName;
            String title = "SOS (" + stnId + ") " + obsName;
            globalAttributes.put("title", title);

            /* institution */
//        globalAttributes.put("institution", "NOAA's National Data Buoy Center (http://www.ndbc.noaa.gov/)");
            globalAttributes.put("institution", "NOAA NDBC");

            /* history */
//        globalAttributes.put("history", "Converted from CSV to OOI CDM compliant NC by " + SosAgent.class.getName());
            globalAttributes.put("history", "Converted from CSV to OOI CDM by " + SosAgent.class.getName());

            /* references */
//        globalAttributes.put("references", "[" + queryUrl + "; http://www.ndbc.noaa.gov/; http://www.noaa.gov/]");
            globalAttributes.put("references", "http://sdf.ndbc.noaa.gov/sos/");

            /* conventions - from schema */
//            globalAttributes.put("Conventions", "CF-1.5");

            /* source */
//        globalAttributes.put("source", "NDBC SOS");
            globalAttributes.put("source", "Sensor Observation Service (http://sdf.ndbc.noaa.gov/sos/server.php?)");

            /* Data URL */
            /* CAN'T have this because it changes every update and we don't have a way of merging attributes across multiple updates */
//            globalAttributes.put("data_url", data_url);


            /* Add each attribute */
            for (IObservationGroup o : obsList) {
                o.addAttributes(globalAttributes);
            }

        } catch (MalformedURLException ex) {
            log.error("Given URL cannot be parsed:  + url", ex);
        } catch (IOException ex) {
            log.error("General IO exception.  Please see stack trace", ex);
        } finally {
            if (null != csvReader) {
                try {
                    csvReader.close();
                } catch (IOException e) { /* NO-OP */

                }
            } else if (null != rdr) {
                try {
                    rdr.close();
                } catch (IOException e) { /* NO-OP */

                }
            }
        }


        return obsList;
    }

    /**
     * Converts the given list of <code>IObservationGroup</code>s to a {@link NetcdfDataset}, breaks that dataset into manageable sections
     * and sends those data "chunks" to the ingestion service.
     * 
     * @param obsList
     *            a group of observations as a list of <code>IObservationGroup</code> objects
     *            
     * @return TODO:
     * 
     * @see #obs2Ncds(IObservationGroup...)
     * @see #sendNetcdfDataset(NetcdfDataset, String)
     * @see #sendNetcdfDataset(NetcdfDataset, String, boolean)
     */
    @Override
    public String[] processDataset(IObservationGroup... obsList) {
        List<String> ret = new ArrayList<String>();
        NetcdfDataset ncds = null;
        try {
            ncds = obs2Ncds(obsList);
            if (ncds != null) {
                /* Send the dataset via the send dataset method of AbstractDatasetAgent */
                ret.add(this.sendNetcdfDataset(ncds, "ingest"));
            } else {
                /* Send the an error via the send dataset method of AbstractDatasetAgent */
                String err = "Abort from this update:: The returned NetcdfDataset is null";
                this.sendDataErrorMsg(StatusCode.AGENT_ERROR, err);
                ret.add(err);
            }
        } catch (IonException ex) {
            ret.add(AgentUtils.getStackTraceString(ex));
        }
        return ret.toArray(new String[0]);
    }

    /*****************************************************************************************************************/
    /* Testing                                                                                                       */
    /*****************************************************************************************************************/
    public static void main(String[] args) throws IOException {
        try {
            ion.core.IonBootstrap.bootstrap();
        } catch (Exception ex) {
            log.error("Error bootstrapping", ex);
        }
        net.ooici.services.sa.DataSource.EoiDataContextMessage.Builder cBldr = net.ooici.services.sa.DataSource.EoiDataContextMessage.newBuilder();
        cBldr.setSourceType(net.ooici.services.sa.DataSource.SourceType.SOS);
        cBldr.setBaseUrl("http://sdf.ndbc.noaa.gov/sos/server.php?");
        int switcher = 6;
        try {
            switch (switcher) {
                case 1: //test station
                    cBldr.setStartDatetimeMillis(AgentUtils.ISO8601_DATE_FORMAT.parse("2008-08-01T00:00:00Z").getTime());
                    cBldr.setEndDatetimeMillis(AgentUtils.ISO8601_DATE_FORMAT.parse("2008-08-02T00:00:00Z").getTime());
                    cBldr.addProperty("sea_water_temperature");
                    cBldr.addStationId("41012");
                    break;
                case 2: //test glider
                    cBldr.setStartDatetimeMillis(AgentUtils.ISO8601_DATE_FORMAT.parse("2010-07-28T00:00:00Z").getTime());
                    cBldr.setEndDatetimeMillis(AgentUtils.ISO8601_DATE_FORMAT.parse("2010-07-29T00:00:00Z").getTime());
                    cBldr.addProperty("salinity");
                    cBldr.addStationId("48900");
                    break;
                case 3: //test UOP
                    cBldr.setStartDatetimeMillis(AgentUtils.ISO8601_DATE_FORMAT.parse("2011-02-23T00:00:00Z").getTime());
                    cBldr.setEndDatetimeMillis(AgentUtils.ISO8601_DATE_FORMAT.parse("2011-02-24T00:00:00Z").getTime());
                    cBldr.addProperty("air_temperature");
                    cBldr.addStationId("41NT0");
                    break;
                case 4: // "East of Virginia Beach, VA
                    cBldr.setStartDatetimeMillis(AgentUtils.ISO8601_DATE_FORMAT.parse("2011-05-20T00:00:00Z").getTime());
                    cBldr.setEndDatetimeMillis(AgentUtils.ISO8601_DATE_FORMAT.parse("2011-05-22T00:00:00Z").getTime());
//                    cBldr.addProperty("air_temperature");
//                    cBldr.addProperty("sea_water_temperature");
                    cBldr.addProperty("Currents");
//                    cBldr.addProperty("Winds");
//                    cBldr.addProperty("Waves");
                    cBldr.addStationId("44014");
                    break;
                case 5: // Test (Part 1/3) For all variables (most vars present)
                    cBldr.setStartDatetimeMillis(AgentUtils.ISO8601_DATE_FORMAT.parse("2011-05-20T00:00:00Z").getTime());
                    cBldr.setEndDatetimeMillis(AgentUtils.ISO8601_DATE_FORMAT.parse("2011-05-22T00:00:00Z").getTime());
                    cBldr.addProperty("air_pressure_at_sea_level");
//                    cBldr.addProperty("air_temperature");
//                    cBldr.addProperty("Currents");
//                    cBldr.addProperty("Salinity");
//                    cBldr.addProperty("WaterTemperature");
//                    cBldr.addProperty("Winds");
                    cBldr.addStationId("41024");
                    break;
                case 6: // Test (Part 2/3) For all variables (conductivity + waves)
                    cBldr.setStartDatetimeMillis(AgentUtils.ISO8601_DATE_FORMAT.parse("2011-05-20T00:00:00Z").getTime());
                    cBldr.setEndDatetimeMillis(AgentUtils.ISO8601_DATE_FORMAT.parse("2011-05-22T00:00:00Z").getTime());
//                    cBldr.addProperty("air_pressure_at_sea_level");
//                    cBldr.addProperty("air_temperature");
                    cBldr.addProperty("Currents"); // FAILED
//                    cBldr.addProperty("sea_water_electrical_conductivity");
//                    cBldr.addProperty("WaterTemperature");
//                    cBldr.addProperty("Waves"); // FAILED
//                    cBldr.addProperty("Winds");
                    cBldr.addStationId("44013");
                    break;
                case 7: // Test (Part 3/3) For all variables (water level only) 
                    cBldr.setStartDatetimeMillis(AgentUtils.ISO8601_DATE_FORMAT.parse("2011-05-20T00:00:00Z").getTime());
                    cBldr.setEndDatetimeMillis(AgentUtils.ISO8601_DATE_FORMAT.parse("2011-05-22T00:00:00Z").getTime());
                    cBldr.addProperty("WaterLevel");
                    cBldr.addStationId("44402");
                    break;

            }
        } catch (ParseException ex) {
            throw new IOException("Error parsing time strings", ex);
        }

        net.ooici.services.sa.DataSource.EoiDataContextMessage context = cBldr.build();

        net.ooici.eoi.datasetagent.IDatasetAgent agent = net.ooici.eoi.datasetagent.AgentFactory.getDatasetAgent(context.getSourceType());
        agent.setAgentRunType(AgentRunType.TEST_WRITE_NC);
//        agent.setAgentRunType(AgentRunType.TEST_WRITE_OOICDM);

        /* Set the maximum size for retrieving/sending - default is 5mb */
//        agent.setMaxSize(50);//super-duper small

//        java.util.HashMap<String, String> connInfo = new java.util.HashMap<String, String>();
//        connInfo.put("exchange", "eoitest");
//        connInfo.put("service", "eoi_ingest");
//        connInfo.put("server", "localhost");
//        connInfo.put("topic", "magnet.topic");
        java.util.HashMap<String, String> connInfo = null;
        try {
            connInfo = IonUtils.parseProperties();
        } catch (IOException ex) {
            log.error("Error parsing \"ooici-conn.properties\" cannot continue.", ex);
            System.exit(1);
        }
        net.ooici.core.container.Container.Structure struct = AgentUtils.getUpdateInitStructure(GPBWrapper.Factory(context));
        String[] result = agent.doUpdate(struct, connInfo);
        log.debug("Response:");
        for (String s : result) {
            log.debug(s);
        }
//        String outdir = "output/sos/";
//        if (!new File(outdir).exists()) {
//            new File(outdir).mkdirs();
//        }
//        String outName = "SOS_Test.nc";
//        try {
//            log.info("Writing NC output to [" + outdir + outName + "]...");
//            ucar.nc2.FileWriter.writeToFile(dataset, outdir + outName);
//        } catch (IOException ex) {
//            log.warn("Could not write NC to file: " + outdir + outName, ex);
//        }
    }
}
