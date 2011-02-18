/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package net.ooici.eoi.datasetagent.impl;

import cern.colt.Timer;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.ooici.eoi.datasetagent.AbstractNcAgent;
import net.ooici.eoi.datasetagent.DataSourceRequestKeys;
import net.ooici.eoi.netcdf.NcdsFactory;
import net.ooici.eoi.netcdf.NcdsFactory.NcdsTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.ma2.InvalidRangeException;
import ucar.ma2.Section;
import ucar.nc2.Attribute;
import ucar.nc2.Dimension;
import ucar.nc2.Variable;
import ucar.nc2.constants.AxisType;
import ucar.nc2.dataset.CoordinateAxis;
import ucar.nc2.dataset.CoordinateAxis1D;
import ucar.nc2.dataset.CoordinateAxis1DTime;
import ucar.nc2.dataset.NetcdfDataset;
import ucar.nc2.dataset.VariableDS;

/**
 *
 * @author cmueller
 */
public class NcGridAgent extends AbstractNcAgent {

    private static final Logger log = LoggerFactory.getLogger(NcGridAgent.class);
    private Date sTime = null;
    private Date eTime = null;

    public String buildRequest(net.ooici.services.sa.DataSource.EoiDataContext context) {
        /* Store the sTime and eTime for later */
        String sTimeString = context.getStartTime();
        String eTimeString = context.getEndTime();
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
        /* Return the dataset URL */
        return context.getDatasetUrl();
    }

    public Object acquireData(String request) {
        if (request.startsWith("ftp://")) {
            /* Pull the file and temporarilly store it locally */
            /* Change "request" to the location of the local file */
        }

        NetcdfDataset ncds = null;
        try {
            ncds = NetcdfDataset.openDataset(request);
        } catch (IOException ex) {
            log.error("Error opening dataset \"" + request + "\"", ex);
        }
        return ncds;
    }
    private static final List<String> fixedVarAtts = new ArrayList<String>();

    static {
        fixedVarAtts.add("standard_name".toLowerCase());
        fixedVarAtts.add("axis".toLowerCase());
        fixedVarAtts.add("_CoordinateAxisType".toLowerCase());
    }
    private static String LAT = "lat", LON = "lon", TIME = "time", LEVEL = "level";
    private String LAT_NAME = LAT, LON_NAME = LON, LEVEL_NAME = LEVEL, TIME_NAME = TIME;
    private String levelType = "";

    public String[] processDataset(NetcdfDataset ncds) {
        NetcdfDataset ncdsRet = null;
        try {
            ncdsRet = NcdsFactory.getNcdsFromTemplate(NcdsTemplate.GRID);
        } catch (FileNotFoundException ex) {
            log.error("Error creating template NetcdfDataset");
        } catch (IOException ex) {
            log.error("Error creating template NetcdfDataset");
        }

        if (ncdsRet != null) {
            try {
                int iStartTime = -1, iEndTime = -1;

                List<CoordinateAxis> coordAxes = ncds.getCoordinateAxes();
                List<String> coordDims = new ArrayList<String>();
                List<String> origDimNames = new ArrayList<String>();
                for (CoordinateAxis a : coordAxes) {
                    Dimension dim;
                    CoordinateAxis1D ca;
                    CoordinateAxis1DTime cat;
                    AxisType at = a.getAxisType();
                    if (at == null) {
                        continue;
                    }
                    switch (at) {
                        case GeoY:
                        case Lat:
                            dim = ncds.findDimension(a.getName());
                            if (dim != null) {
                                origDimNames.add(LAT_NAME = a.getName());
                                if (!coordDims.contains(a.getDimensionsString())) {
                                    coordDims.add(a.getDimensionsString());
                                }
                                ncdsRet.findDimension(LAT).setLength(dim.getLength());
//                    if (a instanceof CoordinateAxis1D) {
//                        ca = (CoordinateAxis1D) a;
//                    } else {
//                        ca = (CoordinateAxis1D) CoordinateAxis1D.factory(ncds, new VariableDS(null, ncds.findVariable(a.getName()), true));
//                    }
//                    procDirec.getBoundingBox().setiNorth(ca.findCoordElementBounded(procDirec.getBoundingBox().getNorth()));
//                    procDirec.getBoundingBox().setiSouth(ca.findCoordElementBounded(procDirec.getBoundingBox().getSouth()));
                            }
                            break;
                        case GeoX:
                        case Lon:
                            dim = ncds.findDimension(a.getName());
                            if (dim != null) {
                                origDimNames.add(LON_NAME = a.getName());
                                if (!coordDims.contains(a.getDimensionsString())) {
                                    coordDims.add(a.getDimensionsString());
                                }
                                ncdsRet.findDimension(LON).setLength(ncds.findDimension(LON_NAME).getLength());
//                    if (a instanceof CoordinateAxis1D) {
//                        ca = (CoordinateAxis1D) a;
//                    } else {
//                        ca = (CoordinateAxis1D) CoordinateAxis1D.factory(ncds, new VariableDS(null, ncds.findVariable(a.getName()), true));
//                    }
//                    procDirec.getBoundingBox().setiWest(ca.findCoordElementBounded(procDirec.getBoundingBox().getWest()));
//                    procDirec.getBoundingBox().setiEast(ca.findCoordElementBounded(procDirec.getBoundingBox().getEast()));
                            }
                            break;
                        case Height:
                        case Pressure:
                        case GeoZ:
//                    origDimNames.add(LEVEL_NAME = a.getName());
                            dim = ncds.findDimension(a.getName());
                            if (dim != null) {
                                levelType = a.getAxisType().toString();
                                origDimNames.add(LEVEL_NAME = LEVEL = a.getName());
                                if (!coordDims.contains(a.getDimensionsString())) {
                                    coordDims.add(a.getDimensionsString());
                                }
                                ncdsRet.addDimension(null, new Dimension(LEVEL, ncds.findDimension(LEVEL).getLength()));
//                    ncdsRet.findDimension(LEVEL).setLength(ncds.findDimension(LEVEL_NAME).getLength());
                            }
                            break;
                        case Time:
                            dim = ncds.findDimension(a.getName());
                            if (dim != null) {
                                origDimNames.add(TIME_NAME = a.getName());
                                if (!coordDims.contains(a.getDimensionsString())) {
                                    coordDims.add(a.getDimensionsString());
                                }
                                if (a instanceof CoordinateAxis1DTime) {
                                    cat = (CoordinateAxis1DTime) a;
                                } else {
                                    cat = CoordinateAxis1DTime.factory(ncds, new VariableDS(null, ncds.findVariable(a.getName()), true), null);
                                }
                                iStartTime = cat.findTimeIndexFromDate(sTime);
                                iEndTime = cat.findTimeIndexFromDate(eTime);
                            }
                            break;
                        default:
                            if (!coordDims.contains(a.getDimensionsString())) {
                                coordDims.add(a.getDimensionsString());
                            }
                            for (Dimension d : a.getDimensions()) {
                                if (ncdsRet.findDimension(d.getName()) == null) {
                                    ncdsRet.addDimension(null, new Dimension(d.getName(), d.getLength()));
                                }
                            }
                            System.out.println("Unhandled coordinate type {" + a.getAxisType() + "}: " + a.getName());
                    }
                }

                /* Add any other dimensions */
                for (Dimension d : ncds.getDimensions()) {
                    if (!origDimNames.contains(d.getName())) {
                        ncdsRet.addDimension(null, new Dimension(d.getName(), d.getLength()));
                    }
                }
                /* Rebuild the object model */
                ncdsRet.finish();

                System.out.println("Start Time Index: " + iStartTime);
                System.out.println("End Time Index: " + iEndTime);


//        System.out.println(ncdsRet);
//        for (Variable v : ncdsRet.getVariables()) {
//            if (v.getName().toLowerCase().startsWith("ex_")) {
//                ncdsRet.removeVariable(null, v.getName());
//            }
//        }

//        ncdsRet.findDimension("lon").setLength((procDirec.getBoundingBox().getiDeltaX()));
//        ncdsRet.findDimension("lat").setLength((procDirec.getBoundingBox().getiDeltaY()));

                /* Variable subsetting */
                List<String> keepVars = new ArrayList<String>();
//        keepVars.addAll(java.util.Arrays.asList(new String[]{"tau"}));

                Variable nv;
                Section sec;
                Timer timer = new Timer();
                /* calling "getVariables()" from the "referencedFile" instead of directly from the dataset is crucial for persisting scaling/offset information to the new dataset */
                for (Variable v : ncds.getReferencedFile().getVariables()) {
                    timer.reset().start();
                    nv = null;
                    if (coordDims.contains(v.getDimensionsString())) {
                        /* it's a coordinate variable */
                        if (v.getName().equals(LAT_NAME)) {
                            nv = makeVariable(ncdsRet, v, LAT);
                        } else if (v.getName().equals(LON_NAME)) {
                            nv = makeVariable(ncdsRet, v, LON);
                        } else if (v.getName().equals(LEVEL_NAME)) {
                            nv = makeVariable(ncdsRet, v, LEVEL);
                            nv.addAttribute(new Attribute("_CoordinateAxisType", levelType));
                        } else if (v.getName().equals(TIME_NAME)) {
                            nv = makeVariable(ncdsRet, v, TIME);
                        } else {
                            /* Catches variables that use only 1 coordinate dimension - i.e. tau(time)*/
                            if (keepVars.isEmpty() || keepVars.contains(v.getName())) {
                                nv = makeVariable(ncdsRet, v, null);
                            }
                        }
                    } else {
                        if (keepVars.isEmpty() || keepVars.contains(v.getName())) {
                            nv = makeVariable(ncdsRet, v, null);
                        }
                    }

                    if (nv != null) {
                        /* Copy the data into the new netcdf dataset */
                        sec = new Section();
                        for (Dimension d : v.getDimensions()) {
                            if (d.getName().equalsIgnoreCase(TIME_NAME)) {
                                sec.appendRange(iStartTime, iEndTime);
                            } else {
                                sec.appendRange(d.getLength());
                            }
                        }
                        int sz = 1;
                        for (int i : sec.getShape()) {
                            sz *= i;
                        }

                        System.out.print(v.getName() + " reading {~bytes = " + (sz * v.getElementSize()) + "}... ");
                        ucar.ma2.Array arr = v.read(sec);
                        nv.setCachedData(arr);
                        updateVarAtts(nv, v);
                        System.out.println("in " + timer.stop().millis() + " milliseconds");
                    }
                }

                /* Add the global attributes */
                for (Attribute a : ncds.getGlobalAttributes()) {
                    if (!a.getName().equalsIgnoreCase("cf:featuretype") & !a.getName().equalsIgnoreCase("conventions")) {
                        ncdsRet.addAttribute(null, a);
                    }
                }

                /* Rebuild the object model */
                ncdsRet.finish();
            } catch (InvalidRangeException ex) {
                log.error("Error creating NetcdfDataset", ex);
            } catch (IOException ex) {
                log.error("Error creating NetcdfDataset", ex);
            }
        }

        String response = this.sendNetcdfDataset(ncdsRet, "ingest");

        return new String[]{response};
    }

    private Variable makeVariable(NetcdfDataset nc, Variable vFrom, String vName) {
        String dimString = vName;
        if (vName == null) {
            vName = vFrom.getName();
            dimString = vFrom.getDimensionsString().replaceAll(LAT_NAME, LAT).replaceAll(LON_NAME, LON).replaceAll(LEVEL_NAME, LEVEL).replaceAll(TIME_NAME, TIME);
        }
        Variable ret = new VariableDS(nc, null, null, vName, vFrom.getDataType(), dimString, vFrom.getUnitsString(), vFrom.getDescription());
        Variable vtemplate = nc.findVariable(vName);
        if (vtemplate != null) {
            nc.removeVariable(null, vtemplate.getName());
            for (Attribute a : vtemplate.getAttributes()) {
                ret.addAttribute(a);
            }
        }
        nc.addVariable(null, ret);

        return ret;
    }

    private void updateVarAtts(Variable vTo, Variable vFrom) {
        for (Attribute a : vFrom.getAttributes()) {
            if (!fixedVarAtts.contains(a.getName().toLowerCase())) {
                vTo.addAttribute(a);
            }
        }
        vTo.removeAttribute("_CoordinateAxes");
    }

    public static void main(String[] args) {
        try {
            ion.core.IonBootstrap.bootstrap();
        } catch (Exception ex) {
            log.error("Error bootstrapping", ex);
        }
        TEST_CONTEXT_GRID.put(DataSourceRequestKeys.SOURCE_TYPE, new String[]{"NC_GRID"});
//        TEST_CONTEXT_GRID.put(DataSourceRequestKeys.BASE_URL, new String[] {"input/grid.nc"});
//        //yyyy-MM-dd'T'HH:mm:ss'Z'
//        TEST_CONTEXT_GRID.put(DataSourceRequestKeys.START_TIME, new String[] {"2006-09-28T00:00:00Z"});
//        TEST_CONTEXT_GRID.put(DataSourceRequestKeys.END_TIME, new String[] {"2006-09-29T00:00:00Z"});

//        TEST_CONTEXT_GRID.put(DataSourceRequestKeys.BASE_URL, new String[] {"http://thredds1.pfeg.noaa.gov:8080/thredds/dodsC/satellite/CM/vsfc/hday"});
//        TEST_CONTEXT_GRID.put(DataSourceRequestKeys.START_TIME, new String[] {"2009-01-05T19:00:00Z"});
//        TEST_CONTEXT_GRID.put(DataSourceRequestKeys.END_TIME, new String[] {"2009-01-06T19:00:00Z"});

//        TEST_CONTEXT_GRID.put(DataSourceRequestKeys.BASE_URL, new String[] {"http://sdf.ndbc.noaa.gov/thredds/dodsC/hfradar_usegc_6km"});
//        TEST_CONTEXT_GRID.put(DataSourceRequestKeys.START_TIME, new String[] {"2011-01-26T00:00:00Z"});
//        TEST_CONTEXT_GRID.put(DataSourceRequestKeys.END_TIME, new String[] {"2011-01-26T20:00:00Z"});

//        TEST_CONTEXT_GRID.put(DataSourceRequestKeys.BASE_URL, new String[] {"http://tashtego.marine.rutgers.edu:8080/thredds/dodsC/cool/avhrr/bigbight"});
//        TEST_CONTEXT_GRID.put(DataSourceRequestKeys.START_TIME, new String[] {"2011-01-26T00:00:00Z"});
//        TEST_CONTEXT_GRID.put(DataSourceRequestKeys.END_TIME, new String[] {"2011-01-26T20:00:00Z"});

        /* This dataset has issues because of it's size - the server throws the following error when it tries to read the first data variable (salinity) which has 489753000 float values for 1 timestep...
         *
        Start Time Index: 23
        End Time Index: 23
        Depth reading {~bytes = 132}... in 2 milliseconds
        Y reading {~bytes = 13192}... in 2 milliseconds
        X reading {~bytes = 18000}... in 1 milliseconds
        MT reading {~bytes = 8}... in 1 milliseconds
        Date reading {~bytes = 8}... in 0 milliseconds
        Latitude reading {~bytes = 59364000}... in 86427 milliseconds
        Longitude reading {~bytes = 59364000}... in 90022 milliseconds
        opendap.dap.DAP2Exception: Method failed:HTTP/1.1 403 Forbidden
        salinity reading {~bytes = 1959012000}... net.ooici.eoi.datasetagent.impl.NcGridAgent.processDataset(){287} - Error creating NetcdfDataset
        java.io.IOException: Method failed:HTTP/1.1 403 Forbidden
        at ucar.nc2.dods.DODSNetcdfFile.readData(DODSNetcdfFile.java:1330)
        at ucar.nc2.Variable.reallyRead(Variable.java:846)
        at ucar.nc2.Variable._read(Variable.java:832)
        at ucar.nc2.Variable.read(Variable.java:644)
        at net.ooici.eoi.datasetagent.impl.NcGridAgent.processDataset(NcGridAgent.java:268)
        at net.ooici.eoi.datasetagent.AbstractNcAgent.processDataset(AbstractNcAgent.java:33)
        at net.ooici.eoi.datasetagent.AbstractDatasetAgent.doUpdate(AbstractDatasetAgent.java:29)
        at net.ooici.eoi.datasetagent.impl.NcGridAgent.main(NcGridAgent.java:347)
        net.ooici.eoi.datasetagent.impl.NcGridAgent.main(){353} - Writing NC output to [output/ncgrid/nc_grid.nc]...
        at opendap.dap.DConnect2.openConnection(DConnect2.java:228)
        at opendap.dap.DConnect2.getData(DConnect2.java:708)
        at opendap.dap.DConnect2.getData(DConnect2.java:988)
        at ucar.nc2.dods.DODSNetcdfFile.readDataDDSfromServer(DODSNetcdfFile.java:1159)
        at ucar.nc2.dods.DODSNetcdfFile.readData(DODSNetcdfFile.java:1323)
        at ucar.nc2.Variable.reallyRead(Variable.java:846)
        at ucar.nc2.Variable._read(Variable.java:832)
        at ucar.nc2.Variable.read(Variable.java:644)
        at net.ooici.eoi.datasetagent.impl.NcGridAgent.processDataset(NcGridAgent.java:268)
        at net.ooici.eoi.datasetagent.AbstractNcAgent.processDataset(AbstractNcAgent.java:33)
        at net.ooici.eoi.datasetagent.AbstractDatasetAgent.doUpdate(AbstractDatasetAgent.java:29)
        at net.ooici.eoi.datasetagent.impl.NcGridAgent.main(NcGridAgent.java:347)
         *
         */
//        TEST_CONTEXT_GRID.put(DataSourceRequestKeys.BASE_URL, new String[]{"http://tds.hycom.org/thredds/dodsC/GLBa0.08/expt_90.9/2011"});
//        TEST_CONTEXT_GRID.put(DataSourceRequestKeys.START_TIME, new String[]{"2011-01-26T00:00:00Z"});
//        TEST_CONTEXT_GRID.put(DataSourceRequestKeys.END_TIME, new String[]{"2011-01-26T00:00:00Z"});

//        Map<String, String[]> context = TEST_CONTEXT_GRID;
        net.ooici.services.sa.DataSource.EoiDataContext.Builder cBldr = net.ooici.services.sa.DataSource.EoiDataContext.newBuilder();
        cBldr.setSourceType(net.ooici.services.sa.DataSource.EoiDataContext.SourceType.NETCDF_C);
        cBldr.setBaseUrl("http://sdf.ndbc.noaa.gov/thredds/dodsC/hfradar_usegc_6km");
        cBldr.setStartTime("2011-01-26T00:00:00Z");
        cBldr.setEndTime("2011-01-26T20:00:00Z");
        net.ooici.services.sa.DataSource.EoiDataContext context = cBldr.build();

        net.ooici.eoi.datasetagent.IDatasetAgent agent = net.ooici.eoi.datasetagent.AgentFactory.getDatasetAgent(context.getSourceType());
        agent.setTesting(true);

        java.util.HashMap<String, String> connInfo = new java.util.HashMap<String, String>();
        connInfo.put("exchange", "eoitest");
        connInfo.put("service", "eoi_ingest");
        connInfo.put("server", "macpro");
        connInfo.put("topic", "magnet.topic");
        String[] result = agent.doUpdate(context, connInfo);
        log.debug("Response:");
        for (String s : result) {
            log.debug(s);
        }
//        if (!new File(outDir).exists()) {
//            new File(outDir).mkdirs();
//        }
//        String outName = "nc_grid.nc";
//        try {
//            log.info("Writing NC output to [" + outDir + outName + "]...");
//            ucar.nc2.FileWriter.writeToFile(dataset, outDir + outName);
//        } catch (IOException ex) {
//            log.warn("Could not write NC to file: " + outDir + outName, ex);
//        }
    }
    private static Map<String, String[]> TEST_CONTEXT_GRID = new HashMap<String, String[]>();
    private static String outDir = "output/ncgrid/";
}
