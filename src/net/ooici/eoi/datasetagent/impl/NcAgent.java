/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package net.ooici.eoi.datasetagent.impl;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;


import net.ooici.eoi.datasetagent.AbstractNcAgent;
import net.ooici.eoi.datasetagent.AgentUtils;
import net.ooici.eoi.netcdf.NcDumpParse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.constants.AxisType;
import ucar.nc2.dataset.CoordinateAxis;
import ucar.nc2.dataset.CoordinateAxis1DTime;
import ucar.nc2.dataset.NetcdfDataset;

/**
 *
 * @author cmueller
 */
public class NcAgent extends AbstractNcAgent {

    private static final Logger log = LoggerFactory.getLogger(NcAgent.class);
    private Date sTime = null, eTime = null;

    @Override
    public String buildRequest(net.ooici.services.sa.DataSource.EoiDataContextMessage context) {
        String ncmlTemplate = context.getNcmlMask();
        String ncdsLoc = context.getDatasetUrl();
        try {
            sTime = AgentUtils.ISO8601_DATE_FORMAT.parse(context.getStartTime());
        } catch (ParseException ex) {
            //log.error("Error parsing start time - first available time will be used", ex);
            sTime = null;
        }
        try {
            eTime = AgentUtils.ISO8601_DATE_FORMAT.parse(context.getEndTime());
        } catch (ParseException ex) {
            //log.error("Error parsing end time - last available time will be used", ex);
            eTime = null;
        }

        String ncmlPath = buildNcmlMask(ncmlTemplate, ncdsLoc);
        log.debug(ncmlPath);
        return ncmlPath;
    }

    @Override
    public Object acquireData(String request) {
        NetcdfDataset ncds = null;
        try {
            ncds = NetcdfDataset.openDataset(request);
        } catch (IOException ex) {
            log.error("Error opening dataset \"" + request + "\"", ex);
        }
//        log.debug("\n" + ncds);
        return ncds;
    }

    @Override
    public String[] processDataset(NetcdfDataset ncds) {
        if (sTime != null & eTime != null) {
            /** TODO: Figure out how to deal with sTime and eTime.
             * Ideally, we'd find a way to 'remove' the unwanted times from the dataset, but not sure if this is possible
             * This would allow the 'sendNetcdfDataset' method to stay very generic (since obs requests will already have dealt with time)
             */
            int sti = -1, eti = -1;
            String tdim = "";
            CoordinateAxis ca = ncds.findCoordinateAxis(AxisType.Time);
            CoordinateAxis1DTime cat = null;
            boolean warn = false;
            Throwable thrown = null;
            ucar.ma2.Range trng = null;
            if (ca != null) {
                if (ca instanceof CoordinateAxis1DTime) {
                    cat = (CoordinateAxis1DTime) ca;
                } else {
                    try {
                        cat = CoordinateAxis1DTime.factory(ncds, new ucar.nc2.dataset.VariableDS(null, ncds.findVariable(ca.getName()), true), null);
                    } catch (IOException ex) {
                        warn = true;
                        thrown = ex;
                    }
                }
                if (cat != null) {
                    tdim = cat.getName();
                    sti = cat.findTimeIndexFromDate(sTime);
                    eti = cat.findTimeIndexFromDate(eTime);
                    try {
                        trng = new ucar.ma2.Range(tdim, sti, eti);
                    } catch (InvalidRangeException ex) {
                        warn = true;
                        thrown = ex;
                    }
                } else {
                    warn = true;
                }
            } else {
                warn = true;
            }
            if (warn) {
                if (thrown != null) {
                    log.warn("Error determining time axis - full time range will be used", thrown);
                } else {
                    log.warn("Error determining time axis - full time range will be used");
                }
            }
            this.addSubRange(trng);
//            System.out.println((trng != null) ? trng.getName() + "=" + trng.toString() : "no trng");
        }

        String response = this.sendNetcdfDataset(ncds, "ingest");

        return new String[]{response};
    }

    private String buildNcmlMask(String content, String ncdsLocation) {
        BufferedWriter writer = null;
        String temploc = null;
        try {
            content = content.replace("***lochold***", ncdsLocation);
            File tempFile = File.createTempFile("ooi", ".ncml");
            tempFile.deleteOnExit();
            temploc = tempFile.getCanonicalPath();
            writer = new BufferedWriter(new FileWriter(tempFile));
            writer.write(content);
        } catch (IOException ex) {
            log.error("Error generating ncml mask for dataset at \"" + ncdsLocation + "\"");
        } finally {
            try {
                writer.close();
            } catch (IOException ex) {
                log.error("Error closing ncml template writer");
            }
        }

        return temploc;
    }

    public static void main(String[] args) throws IOException {
        try {
            ion.core.IonBootstrap.bootstrap();
        } catch (Exception ex) {
            log.error("Error bootstrapping", ex);
        }

//        manualTesting();

//        generateSamples();

        generateMetadata();

    }

    private static void generateMetadata() throws IOException {
        /** For each of the "R1" netcdf datasets (either local or remote)
         *
         * 1. get the last timestep of the data
         * 2. get the list of global-attributes
         * 3. build a delimited string with the following structure:
         *      attribute_1, attribute_2, attribute_3, ..., attribute_n
         *      value_1, value_2, value_3, ..., value_n
         *
         */
//        String[] datasetList = new String[]{"http://nomads.ncep.noaa.gov:9090/dods/nam/nam20110303/nam1hr_00z",
//                                            "http://thredds1.pfeg.noaa.gov/thredds/dodsC/satellite/GR/ssta/1day",
//                                            "http://tashtego.marine.rutgers.edu:8080/thredds/dodsC/cool/avhrr/bigbight/2010"};

        FileReader rdr = new FileReader(new File("netcdf_metadata_input.txt"));
        Properties props = new Properties();
        props.load(rdr);
        

        Map<String, Map<String, String>> datasets = new TreeMap<String, Map<String,String>>(); /* Maps dataset name to an attributes map */
        List<String> metaLookup = new ArrayList<String>();
        
        /* Front-load the metadata list with the OOI required metadata */
        metaLookup.add("title");
        metaLookup.add("institution");
        metaLookup.add("source");
        metaLookup.add("history");
        metaLookup.add("references");
        metaLookup.add("Conventions");
        metaLookup.add("summary");
        metaLookup.add("comment");
        metaLookup.add("data_url");
        metaLookup.add("ion_time_coverage_start");
        metaLookup.add("ion_time_coverage_end");
        metaLookup.add("ion_geospatial_lat_min");
        metaLookup.add("ion_geospatial_lat_max");
        metaLookup.add("ion_geospatial_lon_min");
        metaLookup.add("ion_geospatial_lon_max");
        metaLookup.add("ion_geospatial_vertical_min");
        metaLookup.add("ion_geospatial_vertical_max");
        metaLookup.add("ion_geospatial_vertical_positive");
            
        /* For now, don't add anything - this process will help us figure out what needs to be added */
        String ncmlmask = "<netcdf xmlns=\"http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2\" location=\"***lochold***\"></netcdf>";
        String src = null;
        String url = null;
        String usrHome = System.getProperty("user.home");
        for (Object o : props.keySet()) {
//        for (String dsUrl : datasetList) {
            /* Get the K/V pair */
            src = o.toString();
            url = props.getProperty(src);
            url = (url.startsWith("~")) ? url.replace("~", usrHome) : url;
            
            System.out.println("Getting ncdump for dataset @ " + url);
            
            /* Acquire metadata for the datasource's url */
            net.ooici.services.sa.DataSource.EoiDataContextMessage.Builder cBldr = net.ooici.services.sa.DataSource.EoiDataContextMessage.newBuilder();
            cBldr.setSourceType(net.ooici.services.sa.DataSource.SourceType.NETCDF_S);
//            cBldr.setDatasetUrl(dsUrl).setNcmlMask(ncmlmask);
            cBldr.setDatasetUrl(url).setNcmlMask(ncmlmask);
            cBldr.setStartTime("");
            cBldr.setEndTime("");

            String[] resp = null;
            try {
                resp = runAgent(cBldr.build(), true);
            } catch (Exception e) {
                e.printStackTrace();
                datasets.put(src + " (FAILED)", null);
                continue;
            }
            
            
            System.out.println(".....");
//            System.out.println("\n\nDataSource:\t" + src + "\n-------------------------------------\n" + NcDumpParse.parseToDelimited(resp[0]));
//            Map<String, String> metadataMap = NcDumpParse.parseToMap(resp[0]);
//            TreeMap<String, String> sortedMetadata = new TreeMap<String, String>(metadataMap);
//            
//            for (Object key : sortedMetadata.keySet()) {
//                System.out.println(key.toString());
//            }
            Map<String, String> dsMeta = NcDumpParse.parseToMap(resp[0]);
            datasets.put(src, dsMeta);
            
            
            /* TODO: Eventually we can make this loop external and perform a sort beforehand.
             *       this sort would frontload attributes which are found more frequently
             *       across multiple datasets
             */
            for (String key : dsMeta.keySet()) {
                if (!metaLookup.contains(key)) {
                    metaLookup.add(key);
                }
            }
                
        }
        
        
        /** Write the CSV output */
        String NEW_LINE = System.getProperty("line.separator");
        StringBuilder sb = new StringBuilder();
        
        /* TODO: Step 1: add header data here */
        sb.append("Dataset Name");
        for (String metaName : metaLookup) {
            sb.append("|");
            sb.append(metaName);
//            sb.append('"');
//            sb.append(metaName.replaceAll(Pattern.quote("\""), "\"\""));
//            sb.append('"');
        }
        
        /* Step 2: Add each row of data */
        for (String dsName : datasets.keySet()) {
            Map<String, String> dsMeta = datasets.get(dsName);
            sb.append(NEW_LINE);
            sb.append(dsName);
//            sb.append('"');
//            sb.append(dsName.replaceAll(Pattern.quote("\""), "\"\""));
//            sb.append('"');
            String metaValue = null;
            for (String metaName : metaLookup) {
                sb.append("|");
                if (null != dsMeta && null != (metaValue = dsMeta.get(metaName))) {
                    sb.append(metaValue);
                    /* To ensure correct formatting, change all existing double quotes
                     * to two double quotes, and surround the whole cell value with
                     * double quotes...
                     */
//                    sb.append('"');
//                    sb.append(metaValue.replaceAll(Pattern.quote("\""), "\"\""));
//                    sb.append('"');
                }
            }
            
        }

        
        System.out.println(NEW_LINE + NEW_LINE + "********************************************************");
        System.out.println(sb.toString());
        System.out.println(NEW_LINE + "********************************************************");

    }

    private static void generateSamples() {
    }

    private static void manualTesting() throws IOException {
        /* the ncml mask to use*/
        /* for NAM - WARNING!!  This is a HUGE file... not fully supported on the ingest side yet... */
        String ncmlmask = "<?xml version=\"1.0\" encoding=\"UTF-8\"?> <netcdf xmlns=\"http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2\" location=\"***lochold***\"> <variable name=\"time\"> <attribute name=\"standard_name\" value=\"time\" /> </variable> <variable name=\"lat\"> <attribute name=\"standard_name\" value=\"latitude\" /> <attribute name=\"units\" value=\"degree_north\" /> </variable> <variable name=\"lon\"> <attribute name=\"standard_name\" value=\"longitude\" /> <attribute name=\"units\" value=\"degree_east\" /> </variable> </netcdf>";
        String dataurl = "http://nomads.ncep.noaa.gov:9090/dods/nam/nam20110303/nam1hr_00z";
        String sTime = "";
        String eTime = "";
        long maxSize = -1;

        /* for HiOOS Gliders */
//        ncmlmask = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><netcdf xmlns=\"http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2\" location=\"***lochold***\"><variable name=\"pressure\"><attribute name=\"coordinates\" value=\"time longitude latitude depth\"/></variable><variable name=\"temp\"><attribute name=\"coordinates\" value=\"time longitude latitude depth\"/></variable><variable name=\"conductivity\"><attribute name=\"coordinates\" value=\"time longitude latitude depth\"/></variable><variable name=\"salinity\"><attribute name=\"coordinates\" value=\"time longitude latitude depth\"/></variable><variable name=\"density\"><attribute name=\"coordinates\" value=\"time longitude latitude depth\"/></variable></netcdf>";
//        dataurl = "http://oos.soest.hawaii.edu/thredds/dodsC/hioos/glider/sg139_8/p1390001.nc";
//        sTime = "";
//        eTime = "";

//        /* for HiOOS Gliders Aggregate!!  :-) NOTE: ***lochold*** inside aggregation element and dataurl is to the parent directory, not the dataset */
//        /* TODO: This appears to be an issue as the 'scan' ncml element doesn't appear to work against remote directories...  need to manually create list!! */
//        ncmlmask = "<netcdf xmlns=\"http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2\"><aggregation dimName=\"time\" type=\"joinExisting\"><scan location=\"***lochold***\" suffix=\".nc\" /></aggregation></netcdf>";
//        dataurl = "http://oos.soest.hawaii.edu/thredds/dodsC/hioos/glider/sg139_8/";
//        sTime = "";
//        eTime = "";

        /* HiOOS HFRADAR */
//        ncmlmask = "<netcdf xmlns=\"http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2\" location=\"***lochold***\"></netcdf>";
//        dataurl = "http://oos.soest.hawaii.edu/thredds/dodsC/hioos/hfr/kak/2011/02/RDL_kak_2011_032_0000.nc";


        /* Generic testing */
//        ncmlmask = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><netcdf xmlns=\"http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2\" location=\"***lochold***\"></netcdf>";
//        dataurl = "http://thredds1.pfeg.noaa.gov/thredds/dodsC/satellite/GR/ssta/1day";
//        sTime = "2011-02-01T00:00:00Z";
//        eTime = "2011-02-02T00:00:00Z";
//        maxSize = 33554432;//for pfeg ==> all geospatial (1 time) = 32mb

        /* Local testing */
//        ncmlmask = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><netcdf xmlns=\"http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2\" location=\"***lochold***\"></netcdf>";
//        dataurl = "/Users/cmueller/Development/JAVA/workspace_nb/eoi-agents/output/usgs/USGS_Test.nc";
//        sTime = "2011-01-29T00:00:00Z";
//        eTime = "2011-01-31T00:00:00Z";

        /* More Local testing */
//        ncmlmask = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><netcdf xmlns=\"http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2\" location=\"***lochold***\"></netcdf>";
//        dataurl = "/Users/cmueller/User_Data/Shared_Datasets/NCOM/ncom_glb_scs_2007050700.nc";
//        sTime = "2007-05-07T00:00:00Z";
//        eTime = "2007-05-08T00:00:00Z";

        /* Rutgers ROMS */
//        ncmlmask = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><netcdf xmlns=\"http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2\" location=\"***lochold***\"></netcdf>";
//        dataurl = "http://tashtego.marine.rutgers.edu:8080/thredds/dodsC/roms/espresso/2009_da/his";

        net.ooici.services.sa.DataSource.EoiDataContextMessage.Builder cBldr = net.ooici.services.sa.DataSource.EoiDataContextMessage.newBuilder();
        cBldr.setSourceType(net.ooici.services.sa.DataSource.SourceType.NETCDF_S);
        cBldr.setDatasetUrl(dataurl).setNcmlMask(ncmlmask);
        cBldr.setStartTime(sTime);
        cBldr.setEndTime(eTime);


        runAgent(cBldr.build(), false);
    }

    private static String[] runAgent(net.ooici.services.sa.DataSource.EoiDataContextMessage context, boolean isTesting) throws IOException {
        net.ooici.eoi.datasetagent.IDatasetAgent agent = net.ooici.eoi.datasetagent.AgentFactory.getDatasetAgent(context.getSourceType());
        agent.setTesting(isTesting);

        /* Set the maximum size for retrieving/sending - default is 5mb */
//        agent.setMaxSize(1048576);//1mb
//        agent.setMaxSize(67874688);//~64mb
//        agent.setMaxSize(30000);//pretty small
//        agent.setMaxSize(1500);//very small
//        agent.setMaxSize(150);//super small

//        agent.setMaxSize(maxSize);//ds defined

//        java.util.HashMap<String, String> connInfo = new java.util.HashMap<String, String>();
//        connInfo.put("exchange", "eoitest");
//        connInfo.put("service", "eoi_ingest");
//        connInfo.put("server", "localhost");
//        connInfo.put("topic", "magnet.topic");
        java.util.HashMap<String, String> connInfo = null;
        try {
            connInfo = net.ooici.IonUtils.parseProperties();
        } catch (IOException ex) {
            log.error("Error parsing \"ooici-conn.properties\" cannot continue.", ex);
            System.exit(1);
        }
        String[] result = agent.doUpdate(context, connInfo);
//        log.debug("Response:");
        for (String s : result) {
//            log.debug(s);
        }
        return result;
    }
}
