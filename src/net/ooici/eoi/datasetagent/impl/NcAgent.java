/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package net.ooici.eoi.datasetagent.impl;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import net.ooici.eoi.datasetagent.AbstractNcAgent;
import net.ooici.eoi.datasetagent.DataSourceRequestKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.nc2.dataset.NetcdfDataset;

/**
 *
 * @author cmueller
 */
public class NcAgent extends AbstractNcAgent{
    private static final Logger log = LoggerFactory.getLogger(NcAgent.class);

    public String buildRequest(net.ooici.services.sa.DataSource.EoiDataContext context) {
        String ncmlTemplate = context.getNcmlMask();
        String ncdsLoc = context.getDatasetUrl();
        String ncmlPath = buildNcmlMask(ncmlTemplate, ncdsLoc);
        log.debug(ncmlPath);
        return ncmlPath;
    }

    public Object acquireData(String request) {
        NetcdfDataset ncds = null;
        try {
            ncds = NetcdfDataset.openDataset(request);
        } catch (IOException ex) {
            log.error("Error opening dataset \"" + request + "\"", ex);
        }
        log.debug("\n" + ncds);
        return ncds;
    }

    public NetcdfDataset buildDataset(NetcdfDataset ncds) {
        return ncds;
    }

    private String buildNcmlMask(String content, String ncdsLocation) {
        BufferedWriter writer = null;
        String temploc = null;
        try {
            content = content.replace("***lochold***", ncdsLocation);
            File tempFile = File.createTempFile("ooi", ".ncml");
//            tempFile.deleteOnExit();
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


    public static void main(String[] args) {
        /* the ncml mask to use*/
        /* for NAM */
        String ncmlmask = "<?xml version=\"1.0\" encoding=\"UTF-8\"?> <netcdf xmlns=\"http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2\" location=\"***lochold***\"> <variable name=\"time\" shape=\"time\" type=\"double\"> <attribute name=\"standard_name\" value=\"time\" /> </variable> <variable name=\"lat\" shape=\"lat\" type=\"double\"> <attribute name=\"standard_name\" value=\"latitude\" /> <attribute name=\"units\" value=\"degree_north\" /> </variable> <variable name=\"lon\" shape=\"lon\" type=\"double\"> <attribute name=\"standard_name\" value=\"longitude\" /> <attribute name=\"units\" value=\"degree_east\" /> </variable> </netcdf>";
        String dataurl = "http://nomads.ncep.noaa.gov:9090/dods/nam/nam20110131/nam1hr_00z";

        /* for HiOOS Gliders */
//        ncmlmask = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><netcdf xmlns=\"http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2\" location=\"***lochold***\"><variable name=\"pressure\"><attribute name=\"coordinates\" value=\"time longitude latitude depth\"/></variable><variable name=\"temp\"><attribute name=\"coordinates\" value=\"time longitude latitude depth\"/></variable><variable name=\"conductivity\"><attribute name=\"coordinates\" value=\"time longitude latitude depth\"/></variable><variable name=\"salinity\"><attribute name=\"coordinates\" value=\"time longitude latitude depth\"/></variable><variable name=\"density\"><attribute name=\"coordinates\" value=\"time longitude latitude depth\"/></variable></netcdf>";
//        dataurl = "http://oos.soest.hawaii.edu/thredds/dodsC/hioos/glider/sg139_8/p1390877.nc";

        net.ooici.services.sa.DataSource.EoiDataContext context = net.ooici.services.sa.DataSource.EoiDataContext.newBuilder().setDatasetUrl(dataurl).setNcmlMask(ncmlmask).build();
//
//        Map<String, String[]> context = new HashMap<String, String[]>();
//        context.put(DataSourceRequestKeys.BASE_URL, new String[] {dataurl});
//        context.put("ncml_mask", new String[] {ncmlmask});
        net.ooici.eoi.datasetagent.IDatasetAgent agent = new NcAgent();
        NetcdfDataset dataset = agent.doUpdate(context);

		String outdir = "output/nc/";
        try{
        	if (! new File(outdir).exists()) {
            	new File(outdir).mkdirs();
        	}

            ucar.nc2.FileWriter.writeToFile(dataset, outdir + dataurl.substring(dataurl.lastIndexOf("/") + 1), false, 0, true);
        } catch (IOException ex) {
            log.warn("Could not write NC to file", ex);
        }
    }
}
