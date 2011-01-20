/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package net.ooici.eoi.datasetagent.impl;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import net.ooici.Pair;
import net.ooici.eoi.datasetagent.AbstractAsciiAgent;
import net.ooici.eoi.datasetagent.AgentUtils;
import net.ooici.eoi.datasetagent.DataSourceRequestKeys;
import net.ooici.eoi.datasetagent.obs.IObservationGroup;
import net.ooici.eoi.datasetagent.obs.IObservationGroup.DataType;
import net.ooici.eoi.datasetagent.NcdsFactory;
import net.ooici.eoi.datasetagent.obs.ObservationGroupDupDepthImpl;
import net.ooici.eoi.datasetagent.VariableParams;
import ucar.nc2.dataset.NetcdfDataset;

/**
 *
 * @author cmueller
 */
public class AomlAgent extends AbstractAsciiAgent {

    /** Static Fields */
    static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(AomlAgent.class);
    private static long beginTime = Long.MAX_VALUE;
    private static long endTime = Long.MIN_VALUE;
    protected static final SimpleDateFormat sdf;

    static {
        sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    /* (non-Javadoc)
     * @see net.ooici.agent.abstraction.IDatasetAgent#buildRequest(java.util.Map)
     */
    @Override
    public String buildRequest(Map<String, String[]> context) {
        StringBuilder result = new StringBuilder();


        String baseUrl = (context.containsKey(DataSourceRequestKeys.BASE_URL)) ? context.get(DataSourceRequestKeys.BASE_URL)[0] : null;
        String top = (context.containsKey(DataSourceRequestKeys.TOP)) ? context.get(DataSourceRequestKeys.TOP)[0] : "87.999";
        String bottom = (context.containsKey(DataSourceRequestKeys.BOTTOM)) ? context.get(DataSourceRequestKeys.BOTTOM)[0] : "-87.999";
        String left = (context.containsKey(DataSourceRequestKeys.LEFT)) ? context.get(DataSourceRequestKeys.LEFT)[0] : "-180.0";
        String right = (context.containsKey(DataSourceRequestKeys.RIGHT)) ? context.get(DataSourceRequestKeys.RIGHT)[0] : "180.0";
        String sTimeString = (context.containsKey(DataSourceRequestKeys.START_TIME)) ? context.get(DataSourceRequestKeys.START_TIME)[0] : null;
        String eTimeString = (context.containsKey(DataSourceRequestKeys.END_TIME)) ? context.get(DataSourceRequestKeys.END_TIME)[0] : null;
        String type = (context.containsKey(DataSourceRequestKeys.TYPE)) ? context.get(DataSourceRequestKeys.TYPE)[0] : null;
        String id = (context.containsKey(DataSourceRequestKeys.STATION_ID)) ? context.get(DataSourceRequestKeys.STATION_ID)[0] : "";


        /** Null-checks */
        if (null == baseUrl) {
            throw new IllegalArgumentException("Missing key/value mapping for key: " + DataSourceRequestKeys.BASE_URL + ".  Cannot create data acquisition URL.");
        }

        if (null == eTimeString) {
            throw new IllegalArgumentException("Missing key/value mapping for key: " + DataSourceRequestKeys.START_TIME + ".  Cannot create data acquisition URL.");
        }

        if (null == sTimeString) {
            throw new IllegalArgumentException("Missing key/value mapping for key: " + DataSourceRequestKeys.END_TIME + ".  Cannot create data acquisition URL.");
        }

        if (null == type) {
            throw new IllegalArgumentException("Missing key/value mapping for key: " + DataSourceRequestKeys.TYPE + ".  Cannot create data acquisition URL.");
        }


        /** Pull the dates apart so year/month/day values can be stored */
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
        DateFormat aomlUrlSdf = new SimpleDateFormat("yy:MM:dd");
        aomlUrlSdf.setTimeZone(TimeZone.getTimeZone("UTC"));

        String sTimeArray[] = aomlUrlSdf.format(sTime).split("[\\:]");
        String syy = sTimeArray[0];
        String smm = sTimeArray[1];
        String sdd = sTimeArray[2];
        String eTimeArray[] = aomlUrlSdf.format(eTime).split("[\\:]");
        String eyy = eTimeArray[0];
        String emm = eTimeArray[1];
        String edd = eTimeArray[2];

        /** Grab the typeCode from the given datasource type */
        String typeCode = "";

        if (type.equalsIgnoreCase("xbt")) {
            typeCode = "3";
        } else if (type.equalsIgnoreCase("ctd")) {
            typeCode = "4";
        } else {
            log.error("Invalid datasource type '" + type + "'");
            return null;
        }//TODO: add all remaining datasource types for AOML, here */


        /** Build the query URL */
        result.append(baseUrl);
        result.append("latN=").append(top);
        result.append("&latS=").append(bottom);
        result.append("&lonW=").append(left);
        result.append("&lonE=").append(right);
        result.append("&year=").append(syy);
        result.append("&month=").append(smm);
        result.append("&day=").append(sdd);
        result.append("&year1=").append(eyy);
        result.append("&month1=").append(emm);
        result.append("&day1=").append(edd);
        result.append("&type=").append(typeCode);
        result.append("&id=").append(id);
        result.append("&tipo=1");

        return result.toString();
    }

    /* (non-Javadoc)
     * @see net.ooici.agent.abstraction.IDatasetAgent#acquireData(java.lang.String)
     */
    @Override
    public Object acquireData(String request) {
        /* ASCII data requests are assumed to be basic HTTP post requests, or references to local files */
        log.debug("");
        log.info("Awaiting reply for request [" + request.substring(0, Math.min(100, request.length())) + "...]");

        /* Get the response page which contains the temporary url to the data */
        String tempPage = AgentUtils.getDataString(request);
        /* Parse the actual temporary data url from the response page */
        Pattern pattern = Pattern.compile("http://www.aoml.noaa.gov/phod/trinanes/tmp/[a-z 0-9]+.dat");
        Matcher match = pattern.matcher(tempPage);
        if (!match.find()) {
            log.error("Error, no valid data URL found on page: " + tempPage);
            return null;
        }
        String dataUrl = match.group();
        log.info("DataURL [" + dataUrl.substring(0, Math.min(100, dataUrl.length())) + "...]");


        /* Retrieve the actual data */
        String data = AgentUtils.getDataString(dataUrl);
        log.debug("... acquired raw data: [" + data.substring(0, Math.min(1000, data.length())) + "...]");
        return data;
    }

    @Override
    protected List<IObservationGroup> parseObs(String asciiData) {
        List<IObservationGroup> ogList = new ArrayList<IObservationGroup>();
        Pattern headerPattern =
                Pattern.compile("[A-Z a-z]+\\s[A-Z a-z]+\\s[A-Z a-z]+\\s[A-Z a-z]+\\s[A-Z a-z]+\\s[A-Z a-z]+(\\s[A-Z a-z]+)?");
        Matcher headerMatcher = headerPattern.matcher(asciiData);
        Pattern dataPattern = Pattern.compile("[0-9 .]+\\s-[0-9 .]+\\s[A-Z 0-9]+\\s[0-9]+-[0-9]+-[0-9]+\\s[0-9]+:[0-9]+");
        Matcher dataMatcher = dataPattern.matcher(asciiData);

        String header = null;
        while (headerMatcher.find()) {
            header = asciiData.substring(headerMatcher.start(), headerMatcher.end());
        }
        // System.out.println(header);
        List<Pair<Integer, VariableParams>> dataCols = new ArrayList<Pair<Integer, VariableParams>>();
        String[] dhead = header.split("\n")[1].split("\\s+");
        for (int i = 0; i < dhead.length; i++) {
            if (dhead[i].equalsIgnoreCase("temp")) {
                dataCols.add(new Pair<Integer, VariableParams>(i, VariableParams.SEA_WATER_TEMPERATURE));
            } else if (dhead[i].equalsIgnoreCase("salinity")) {
                dataCols.add(new Pair<Integer, VariableParams>(i, VariableParams.SEA_WATER_SALINITY));
            }
        }

        List<Integer> ints = new ArrayList<Integer>();
        while (dataMatcher.find()) {
            ints.add(dataMatcher.start());
        }


        String t;
        IObservationGroup og = null;
        String stnId = "";
        float lat = -9999, tla = -99991, lon = -9999, tlo = -99991;
        int obsId = 0;
        int time = 0;
        for (int i = 0; i < ints.size(); i++) {
            if (i < ints.size() - 1) {
                t = asciiData.substring(ints.get(i), ints.get(i + 1));
            } else {
                t = asciiData.substring(ints.get(i));
            }
            String[] lines = t.split("\n");
            /* Header Line */
            // System.out.println(lines[0]);
            String[] stnInfo = lines[0].split("\\s+");
            if (og == null | !stnId.equals(stnInfo[2]) | (lat != (tla = Float.valueOf(stnInfo[0])))
                    | lon != (tlo = Float.valueOf(stnInfo[1]))) {
                if (og != null && !ogList.contains(og)) {
                    ogList.add(og);
                }
                og = null;
                stnId = stnInfo[2];
                lat = tla;
                lon = tlo;
                /* AOML data can be unordered - check if the observation group is already in the list */
                for (IObservationGroup s : ogList) {
                    if (s.getStnid().equals(stnId) && s.getLat().equals(lat) && s.getLon().equals(lon)) {
                        og = s;
                        break;
                    }
                }
                /* Totally new observation group */
                if (og == null) {
                    og = new ObservationGroupDupDepthImpl(obsId++, stnId, lat, lon);
                }
            }
            /* get time */
            try {
                time = (int) (sdf.parse(stnInfo[3].concat("T").concat(stnInfo[4].concat("Z"))).getTime() * 0.001);
            } catch (java.text.ParseException ex) {
                time = 0;
            } catch (NumberFormatException ex) {
                time = 0;
            } catch (ArrayIndexOutOfBoundsException ex) {
                time = 0;
            }
            /* add data for depths */
            Pair<Integer, VariableParams> dc;
            float val;
            float d;
            for (int l = 1; l < lines.length; l++) {
                /* Data Lines*/
                // System.out.println("\t".concat(lines[l]));
                String[] dat = lines[l].split("\\s+");
                for (int di = 0; di < dataCols.size(); di++) {
                    dc = dataCols.get(di);
                    /* Get the depth */
                    try {
                        d = Float.valueOf(dat[0]);
                    } catch (NumberFormatException ex) {
                        /* Data not a value... */
                        d = 0.0f;
                    } catch (ArrayIndexOutOfBoundsException ex) {
                        /* If data is missing. */
                        d = 0.0f;
                    }
                    /* Get the data for the current dataCol */
                    try {
                        val = Float.valueOf(dat[dc.getKey()]);
                    } catch (NumberFormatException ex) {
                        /* Data not a value... */
                        val = Float.NaN;
                    } catch (ArrayIndexOutOfBoundsException ex) {
                        /* If data is missing. */
                        val = Float.NaN;
                    }
                    og.addObservation(time, d, val, new VariableParams(dc.getValue(), DataType.FLOAT));
                }
            }
        }
        /* Add the last observation group processed */
        if (og != null && !ogList.contains(og)) {
            ogList.add(og);
        }


        /** Setup Global Attributes to copy into each observation group */
        Map<String, String> globalAttributes = new HashMap<String, String>();

        /* Extract the Global Attributes */
        /* title */
        String queryUrl = "http://www.aoml.noaa.gov/";
        globalAttributes.put("title", "AOML data from \"" + queryUrl + "\"");

        /* history */
        globalAttributes.put("history", "Converted from TSV to OOI CDM compliant NC by " + AomlAgent.class.getName());

        /* references */
        globalAttributes.put("references", "[" + queryUrl + "; http://www.noaa.gov/]");

        /* conventions */
        globalAttributes.put("Conventions", "CF-1.5");

        /* institution */
        globalAttributes.put("institution", "NOAA's Atlantic and Oceanographic Meteorological Laboratory (" + queryUrl + ")");

        /* source */
        globalAttributes.put("source", "NOAA AOML");

        /* Add each attribute */
        for (IObservationGroup s : ogList) {
            s.addAttributes(globalAttributes);
        }

        return ogList;
    }

    @Override
    protected NetcdfDataset obs2Ncds(List<IObservationGroup> observations) {
        List<NetcdfDataset> datasets = new ArrayList<NetcdfDataset>();
        for(IObservationGroup og : observations) {
            if(og.getDepths().length > 1) {
                datasets.add(NcdsFactory.buildStationProfile(og));
            } else {
                datasets.add(NcdsFactory.buildStation(og));
            }
        }
        
        return datasets.get(0);
    }

    /*****************************************************************************************************************/
    /* Testing                                                                                                       */
    /*****************************************************************************************************************/
    public static void main(String[] args) {
        Map<String, String[]> context = TEST_CONTEXT;
        if(false) {//set == true to test xbt
            TEST_CONTEXT.put(DataSourceRequestKeys.TYPE, new String[]{"xbt"});
        }
        net.ooici.eoi.datasetagent.IDatasetAgent agent = net.ooici.eoi.datasetagent.AgentFactory.getDatasetAgent(context.get(DataSourceRequestKeys.SOURCE_NAME)[0]);
        NetcdfDataset dataset = agent.doUpdate(context);
        
        log.debug(dataset.toString());
    }
    private static Map<String, String[]> TEST_CONTEXT = new java.util.HashMap<String, String[]>();

    static {
        /* Request data from one month ago for a length of 10 days.. */
        java.util.GregorianCalendar endTime = new java.util.GregorianCalendar(TimeZone.getTimeZone("UTC"));
        endTime.add(java.util.Calendar.MONTH, -1);
        java.util.GregorianCalendar startTime = (java.util.GregorianCalendar) endTime.clone();
        startTime.add(java.util.Calendar.DAY_OF_MONTH, -10);

        /* Store the example parameters */
        TEST_CONTEXT.put(DataSourceRequestKeys.SOURCE_NAME, new String[]{"AOML"});
        TEST_CONTEXT.put(DataSourceRequestKeys.BASE_URL, new String[]{"http://www.aoml.noaa.gov/cgi-bin/trinanes/datosxbt.cgi?"});
        TEST_CONTEXT.put(DataSourceRequestKeys.LEFT, new String[]{"-82.0"});
        TEST_CONTEXT.put(DataSourceRequestKeys.RIGHT, new String[]{"-60.0"});
        TEST_CONTEXT.put(DataSourceRequestKeys.BOTTOM, new String[]{"31.0"});
        TEST_CONTEXT.put(DataSourceRequestKeys.TOP, new String[]{"47.0"});
        TEST_CONTEXT.put(DataSourceRequestKeys.START_TIME, new String[]{DataSourceRequestKeys.ISO8601_FORMAT.format(startTime.getTime())});
        TEST_CONTEXT.put(DataSourceRequestKeys.END_TIME, new String[]{DataSourceRequestKeys.ISO8601_FORMAT.format(endTime.getTime())});
        TEST_CONTEXT.put(DataSourceRequestKeys.TYPE, new String[]{"ctd"});
    }
}
