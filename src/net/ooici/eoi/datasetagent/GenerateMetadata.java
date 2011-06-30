/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package net.ooici.eoi.datasetagent;


import ion.core.utils.IonUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import net.ooici.Pair;
import net.ooici.core.container.Container;
import net.ooici.eoi.datasetagent.AbstractDatasetAgent.AgentRunType;
import net.ooici.eoi.netcdf.NcDumpParse;
import net.ooici.services.sa.DataSource.SourceType;


/**
 * 
 * @author cmueller
 */
public class GenerateMetadata {

    /* NOTE: this class has been tested against the NDBC_SOS*.dsreg files -- more testing required */
    /* TODO: Sometimes runAgent errors because it returns no observations for some dsreg files -- we might want to abstract the
             values used for startDate and endDate from convertDrcrStructToEoiContextStruct().  Search for variable "yesterday",
             it is currently used to set the start and end date of the update */
    /* TODO: create a method to output the intermediate metadata map to a key value file */
    /* TODO: make the intermediate output from getMetadata sort its outputs according to the metadataLookupList -- requires returning a treemap */
    
    /** Static Fields */
    static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(GenerateMetadata.class);



    public static void main(String[] args) throws FileNotFoundException, IOException, Exception {
        log.debug("starting...");

        
        /* TODO: abstract input arguments from here to make this into a command-line client */
        /* ************ */
        String outputFilepath = "/Users/tlarocque/Desktop/eoi_metadata.csv";
        String metadataHeadersFilepath = "";  /* TODO: substitute this with "netcdf_metadata_headers.txt" when present */
        String dsregDirectory = "/Users/tlarocque/Desktop/dsreg_test/";
        /* ************ */
        
        
        /** Step 1: Read the given location and gather a list of all *.dsreg files present */
        List<File> filepaths = new ArrayList<File>();
        File dsregFile = new File(dsregDirectory);
        if (dsregFile.isDirectory()) {
            File[] dsregList = dsregFile.listFiles(new FilenameFilter() {

                @Override
                public boolean accept(File dir, String name) {
                    return name.endsWith(".dsreg");
                }
                
            });
            filepaths.addAll(Arrays.asList(dsregList));
            
        } else {
            
            filepaths.add(new File(dsregDirectory));
            
        }


        /** FOR EACH *.dsreg FILE.... */
        Map<String, Map<String, String>> nameToMetadataMap = new HashMap<String, Map<String,String>>();
        Pair<Container.Structure, SourceType> ectxPair;
        Map<String, String> metadata;
        
        String filename;
        String filepath;
        for (File file : filepaths) {
            filename = file.getName();
            filepath = file.getAbsolutePath();
            
            /** Step 2: Create an EoiContextMessage Structure from the *.dsreg file */
            ectxPair = AgentUtils.buildEoiContextStructFromDsreg(filepath);

            /** Step 3: Generate a metadata map for this EoiContextMessage */
            metadata = generateMetadata(ectxPair.getKey(), ectxPair.getValue(), filename);
            nameToMetadataMap.put(filename, metadata);
        }

        /** Step 4: Generate the metadata CSV output */
        String metadataCSV = generateMetadataCSV(nameToMetadataMap, metadataHeadersFilepath);
        log.debug("\n\n\n" + metadataCSV + "\n\n\n");
        
        
        FileWriter fw = new FileWriter(new File(outputFilepath));
        try {
            fw.append(metadataCSV);
        } finally {
            fw.flush();
            fw.close();
        }
        
        
        
        log.debug("done! - See \"{}\" for metadata.", outputFilepath);
    }


    public static Map<String, String> generateMetadata(Container.Structure eoiContextStruct, SourceType sourceType, String dataSourceName) throws IOException {

        Map<String, Map<String, String>> datasets = new TreeMap<String, Map<String, String>>(); /* Maps dataset name to an attributes map */


        String[] resp = null;
        try {
            resp = runAgent(eoiContextStruct, sourceType, AgentRunType.TEST_NO_WRITE);
        } catch (Exception e) {
            e.printStackTrace();
            datasets.put(dataSourceName + " (FAILED)", null);
            return null;
        }


        log.debug(".....");
        Map<String, String> dsMeta = NcDumpParse.parseToMap(resp[0]);

        return dsMeta;
    }
    

    public static String generateMetadataCSV(Map<String, Map<String, String>> nameToMetadataMap, String metadataHeadersFilepath)
        throws FileNotFoundException, IOException {
        final String NEW_LINE = System.getProperty("line.separator");
        StringBuilder sb = new StringBuilder();


        /** Step 1: Front-load the metadata list with the existing metadata headers - preserves order of the spreadsheet */
        List<String> metaLookup = loadMetadataLookupList(metadataHeadersFilepath);

        /** Step 2: Load any remaining metadata values into the lookup list */
        for (Map<String, String> metadataMap : nameToMetadataMap.values()) {
            if (null != metadataMap) {

                for (String metadata : metadataMap.keySet()) {
                    if (!metadata.isEmpty() && !metaLookup.contains(metadata)) {

                        metaLookup.add(metadata);

                    }
                }

            }
        }


        /** Step 3: Build the CSV output */

        /* Step 1: add header data here */
        sb.append("Dataset Name");
        for (String metaName : metaLookup) {
            sb.append("|");
            sb.append(metaName);
            // sb.append('"');
            // sb.append(metaName.replaceAll(Pattern.quote("\""), "\"\""));
            // sb.append('"');
        }

        /* Step 2: Add each row of data */
        String dsetName;
        Map<String, String> metadata;
        for (Entry<String, Map<String, String>> entry : nameToMetadataMap.entrySet()) {
            dsetName = entry.getKey();
            metadata = entry.getValue();


            sb.append(NEW_LINE);
            sb.append(dsetName);
            // sb.append('"');
            // sb.append(dsName.replaceAll(Pattern.quote("\""), "\"\""));
            // sb.append('"');
            String metaValue = null;
            for (String metaName : metaLookup) {
                sb.append("|");
                if (null != metadata && null != (metaValue = metadata.get(metaName))) {
                    sb.append(metaValue);
                    /* To ensure correct formatting, change all existing double quotes
                     * to two double quotes, and surround the whole cell value with
                     * double quotes...
                     */
                    // sb.append('"');
                    // sb.append(metaValue.replaceAll(Pattern.quote("\""), "\"\""));
                    // sb.append('"');
                }
            }

        }


        log.debug(new StringBuilder(NEW_LINE)
                     .append(NEW_LINE)
                     .append("********************************************************")
                     .append(sb.toString()).append(NEW_LINE)
                     .append("********************************************************")
                  .toString());

        return sb.toString();
    }


    public static List<String> loadMetadataLookupList(String metadataHeadersFilepath) throws FileNotFoundException, IOException {
        List<String> metaLookup = new ArrayList<String>();

        /** Add metadata headers from the given file if provided */
        if (null != metadataHeadersFilepath) {
            File metadataHeadersFile = new File(metadataHeadersFilepath);
            if (metadataHeadersFile.exists()) {

                BufferedReader brdr = null;
                try {
                    brdr = new BufferedReader(new FileReader(metadataHeadersFile));
                    String line;
                    while ((line = brdr.readLine()) != null) {
                        metaLookup.add(line.trim());
                    }
                } finally {
                    brdr.close();
                }
            } else {
                log.warn("The file specifying the existing metadata (\"{}\") cannot be found.", metadataHeadersFilepath);
            }
        }

        /** Insert default preload headers if nothing was loaded */
        if (metaLookup.isEmpty()) {
            log.warn("Metadata headers list is empty.  Providing \"OOI Minimum\" metadata as defaults.");
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
        }


        return metaLookup;
    }
    
    
    public static String[] runAgent(Container.Structure eoiContextStruct, SourceType sourceType, AgentRunType agentRunType)
        throws IOException {
        /** Retrieve an agent based on the source type of the EoiContextMessage Structure (provided as sourceType) */
        IDatasetAgent agent = AgentFactory.getDatasetAgent(sourceType);
        agent.setAgentRunType(agentRunType);


        /** Retrieve the neccessary connection info */
        java.util.HashMap<String, String> connInfo = null;
        try {
            connInfo = IonUtils.parseProperties();
        } catch (IOException ex) {
            log.error("Error parsing \"ooici-conn.properties\" cannot continue.", ex);
            System.exit(1);
        }

        /** Run the agent... */
        String[] result = agent.doUpdate(eoiContextStruct, connInfo);
        if (log.isDebugEnabled()) {
            for (String s : result) {
                log.debug(s);
            }
        }


        return result;
    }


}