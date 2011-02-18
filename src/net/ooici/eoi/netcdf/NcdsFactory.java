/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package net.ooici.eoi.netcdf;

import net.ooici.eoi.datasetagent.obs.ObservationGroupImpl;
import net.ooici.eoi.datasetagent.obs.IObservationGroup;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.ma2.Array;
import ucar.ma2.ArrayDouble;
import ucar.ma2.ArrayFloat;
import ucar.ma2.ArrayInt;
import ucar.ma2.ArrayLong;
import ucar.ma2.DataType;
import ucar.ma2.IndexIterator;
import ucar.nc2.Attribute;
import ucar.nc2.Variable;
import ucar.nc2.constants.CF;
import ucar.nc2.dataset.NetcdfDataset;
import ucar.nc2.dataset.VariableDS;

/**
 * Factory class with static methods for generating {@link NetcdfDataset} objects from one or more {@link IObservationGroup} objects.
 *
 * @author cmueller
 */
public class NcdsFactory {

    private static final Logger log = LoggerFactory.getLogger(NcdsFactory.class);

    public enum NcdsTemplate {

        GRID("grid.ncml"),
        POINT("point.ncml"),
        PROFILE("profile.ncml"),
        STATION("station.ncml"),
        STATION_MULTI("stationMulti.ncml"),
        STATION_PROFILE("stationProfile.ncml"),
        STATION_PROFILE_MULTI("stationProfileMulti.ncml"),
        TRAJECTORY("trajectory.ncml"),
        TRAJECTORY_PROFILE("trajectoryProfile.ncml");

        String resourceName = null;

        NcdsTemplate(String resourceName) {
            if (null == resourceName || resourceName.isEmpty()) {
                throw new IllegalArgumentException("Argument resourceName cannot be NULL or empty");
            }
            this.resourceName = resourceName;
        }

        public String getResourceName() {
            return resourceName;
        }
    }

    public static NetcdfDataset buildStation(IObservationGroup obsGroup) {
        if (obsGroup.getDepths().length > 1) {
            return buildStationProfile(obsGroup);
        }
        NetcdfDataset ncds = null;
        try {
            /* Instantiate an empty NetcdfDataset object from the template ncml */
            ncds = getNcdsFromTemplate(NcdsTemplate.STATION);

            Map<String, String> allAttributes = new HashMap<String, String>();
            allAttributes.putAll(obsGroup.getAttributes());

            /* Do the station Id */
            ArrayInt.D0 aid = new ArrayInt.D0();
            aid.set(obsGroup.getId());
            ncds.findVariable("stnId").setCachedData(aid);

            /* Do the times */
            Number[] times = obsGroup.getTimes();
            IObservationGroup.DataType tdt = obsGroup.getTimeDataType();
            ncds.findDimension("time").setLength(times.length);
            Variable tvar = ncds.findVariable("time");
            tvar.resetShape();
            tvar.setDataType(getNcDataType(tdt));
            tvar.setCachedData(getNcArray(times, tdt));

            /* Do the lat & lon */
            IObservationGroup.DataType lldt = obsGroup.getLatLonDataType();
            DataType ncdtLl = getNcDataType(lldt);
            Variable laVar = ncds.findVariable("lat");
            laVar.setDataType(ncdtLl);
            laVar.setCachedData(getNcScalar(obsGroup.getLat(), lldt));
            Variable loVar = ncds.findVariable("lon");
            loVar.setDataType(ncdtLl);
            loVar.setCachedData(getNcScalar(obsGroup.getLon(), lldt));

            /* Do the depth */
            IObservationGroup.DataType ddt = obsGroup.getDepthDataType();
            VariableDS zVar = new VariableDS(ncds, null, null, "stnDepth", getNcDataType(ddt), "", "m", "station depth");
            zVar.addAttribute(new Attribute("positive", "down"));
            ncds.addVariable(null, zVar);
            Number depth = obsGroup.getDepths()[0];
            zVar.setCachedData(getNcScalar(obsGroup.getDepths()[0], ddt));

            /* Do the data variables */
            for (VariableParams dn : obsGroup.getDataNames()) {
                DataType ncdtData = getNcDataType(dn.getDataType());
                VariableDS dvar = new VariableDS(ncds, null, null, dn.getShortName(), ncdtData, "time", dn.getUnits(), dn.getDescription());
                dvar.addAttribute(new Attribute(CF.COORDINATES, "time lon lat"));
                dvar.addAttribute(new Attribute(CF.STANDARD_NAME, dn.getStandardName()));
                Array adata = Array.factory(ncdtData, new int[]{times.length});
                IndexIterator aii = adata.getIndexIterator();
                dvar.setCachedData(adata);
                ncds.addVariable(null, dvar);

                for (int ti = 0; ti < times.length; ti++) {
                    putArrayData(aii, ncdtData, obsGroup.getData(dn, times[ti], depth));
                }
            }

            /* Add global attributes */
            for (String key : allAttributes.keySet()) {
                ncds.addAttribute(null, new Attribute(key, allAttributes.get(key)));
            }
        } catch (IOException ex) {
            log.error("Error building station NetcdfDataset", ex);
        } finally {
            if (ncds != null) {
                ncds.finish();
            }
        }

        return ncds;
    }

    public static NetcdfDataset buildStationMulti(IObservationGroup[] obsGroups) {
        if (obsGroups.length == 1) {
            return buildStation(obsGroups[0]);
        }
        throw new UnsupportedOperationException();
    }

    public static NetcdfDataset buildStationProfile(IObservationGroup obsGroup) {
        if (obsGroup.getDepths().length == 1) {
            return buildStation(obsGroup);
        }
        NetcdfDataset ncds = null;
        try {
            /* Instantiate an empty NetcdfDataset object from the template ncml */
            ncds = getNcdsFromTemplate(NcdsTemplate.STATION_PROFILE);

            Map<String, String> allAttributes = new HashMap<String, String>();
            allAttributes.putAll(obsGroup.getAttributes());

            /* Do the station Id */
            ArrayInt.D0 aid = new ArrayInt.D0();
            aid.set(obsGroup.getId());
            ncds.findVariable("stnId").setCachedData(aid);

            /* Do the times */
            Number[] times = obsGroup.getTimes();
            IObservationGroup.DataType tdt = obsGroup.getTimeDataType();
            ncds.findDimension("time").setLength(times.length);
            Variable tvar = ncds.findVariable("time");
            tvar.setDataType(getNcDataType(tdt));
            tvar.setCachedData(getNcArray(times, tdt));

            /* Do the lat & lon */
            IObservationGroup.DataType lldt = obsGroup.getLatLonDataType();
            DataType ncdtLl = getNcDataType(lldt);
            Variable laVar = ncds.findVariable("lat");
            laVar.setDataType(ncdtLl);
            laVar.setCachedData(getNcScalar(obsGroup.getLat(), lldt));
            Variable loVar = ncds.findVariable("lon");
            loVar.setDataType(ncdtLl);
            loVar.setCachedData(getNcScalar(obsGroup.getLon(), lldt));

            /* Do the depths */
            Number[] depths = obsGroup.getDepths();
            ncds.findDimension("z").setLength(depths.length);
            IObservationGroup.DataType ddt = obsGroup.getDepthDataType();
            Variable zVar = ncds.findVariable("z");
            zVar.setDataType(getNcDataType(ddt));
            zVar.setCachedData(getNcArray(depths, ddt));

            /* Do the data variables */
            for (VariableParams dn : obsGroup.getDataNames()) {
                DataType ncdtData = getNcDataType(dn.getDataType());
                VariableDS dvar = new VariableDS(ncds, null, null, dn.getShortName(), ncdtData, "time z", dn.getUnits(), dn.getDescription());
                dvar.addAttribute(new Attribute(CF.COORDINATES, "time lon lat z"));
                dvar.addAttribute(new Attribute(CF.STANDARD_NAME, dn.getStandardName()));
                Array adata = Array.factory(ncdtData, new int[]{times.length, depths.length});
                IndexIterator aii = adata.getIndexIterator();
                dvar.setCachedData(adata);
                ncds.addVariable(null, dvar);

                for (int ti = 0; ti < times.length; ti++) {
                    for (int di = 0; di < depths.length; di++) {
                        putArrayData(aii, ncdtData, obsGroup.getData(dn, times[ti], depths[di]));
                    }
                }
            }

            /* Add global attributes */
            for (String key : allAttributes.keySet()) {
                ncds.addAttribute(null, new Attribute(key, allAttributes.get(key)));
            }
        } catch (IOException ex) {
            log.error("Error building stationProfile NetcdfDataset", ex);
        } finally {
            if (ncds != null) {
                ncds.finish();
            }
        }

        return ncds;
    }

    public static NetcdfDataset buildStationProfileMulti(IObservationGroup[] obsGroups) {
        if (obsGroups.length == 1) {
            return buildStationProfile(obsGroups[0]);
        }
        throw new UnsupportedOperationException();
    }

    public static NetcdfDataset buildTrajectory(IObservationGroup[] obsGroups) {
        NetcdfDataset ncds = null;
        try {
            /* Instantiate an empty NetcdfDataset object from the template ncml */
            ncds = getNcdsFromTemplate(NcdsTemplate.TRAJECTORY);

            int nobs = obsGroups.length;
            List<Number> allDepths = new ArrayList<Number>();
            List<VariableParams> allDn = new ArrayList<VariableParams>();
            int nt = nobs;
            int nd = 0;
            for (IObservationGroup og : obsGroups) {
                nd = Math.max(nd, og.getDepths().length);
                for (Number d : og.getDepths()) {
                    if (!allDepths.contains(d)) {
                        allDepths.add(d);
                    }
                }
                for (VariableParams dn : og.getDataNames()) {
                    if (!allDn.contains(dn)) {
                        allDn.add(dn);
                    }
                }
            }

            /* Do the trajectory ID */

            /* Do the times */
            Number[] times = obsGroups[0].getTimes();
            IObservationGroup.DataType tdt = obsGroups[0].getTimeDataType();
            DataType ncdtTime = getNcDataType(tdt);
            ncds.findDimension("time").setLength(nt);
            Array tarr = Array.factory(ncdtTime, new int[]{nt});
            IndexIterator tii = tarr.getIndexIterator();
            Variable tvar = ncds.findVariable("time");
            tvar.setDataType(getNcDataType(tdt));
            tvar.setCachedData(tarr);

            /* Do the lats */
            IObservationGroup.DataType lldt = obsGroups[0].getLatLonDataType();
            DataType ncdtLl = getNcDataType(lldt);
            Array laarr = Array.factory(ncdtLl, new int[]{nt});
            IndexIterator laii = laarr.getIndexIterator();
            Variable lavar = ncds.findVariable("lat");
            lavar.setDataType(ncdtLl);
            lavar.setCachedData(laarr);

            /* Do the lons */
            Array loarr = Array.factory(ncdtLl, new int[]{nt});
            IndexIterator loii = loarr.getIndexIterator();
            Variable lovar = ncds.findVariable("lon");
            lovar.setDataType(ncdtLl);
            lovar.setCachedData(loarr);

            /* Iterate over the observation groups and fill the data */
            Map<String, String> allAttributes = new HashMap<String, String>();
            IObservationGroup og;
            HashMap<String, IndexIterator> darrs = new HashMap<String, IndexIterator>();
            Number time;
            Number depth = allDepths.get(0);
            for (int obs = 0; obs < nobs; obs++) {
                og = obsGroups[obs];
                time = og.getTimes()[0];
                putArrayData(tii, ncdtTime, time);
                putArrayData(loii, ncdtLl, og.getLon());
                putArrayData(laii, ncdtLl, og.getLat());

                for (VariableParams dn : allDn) {
                    if (og.getDataNames().contains(dn)) {
                        DataType ncdtData = getNcDataType(dn.getDataType());
                        VariableDS dvar = (VariableDS) ncds.findVariable(dn.getShortName());
                        if (dvar == null) {
                            dvar = new VariableDS(ncds, null, null, dn.getShortName(), ncdtData, "time", dn.getUnits(), dn.getDescription());
                            dvar.addAttribute(new Attribute(CF.COORDINATES, "time lon lat"));
//                            dvar.addAttribute(new Attribute("missing_value", missingData));
                            dvar.addAttribute(new Attribute(CF.STANDARD_NAME, dn.getStandardName()));
                            Array darr = Array.factory(ncdtData, new int[]{nt});
                            dvar.setCachedData(darr);

                            darrs.put(dn.getStandardName(), darr.getIndexIterator());
                            ncds.addVariable(null, dvar);
                        }
                        putArrayData(darrs.get(dn.getStandardName()), ncdtData, og.getData(dn, time, depth));

                    } else {
                        /*
                         * station doesn't have this variable - don't believe
                         * this can even happen...
                         *
                         * NOTE: This would indicate a problem with the above processing where the data-name list
                         * has been modified prior to contains-checking
                         */
                    }
                }
            }

            /* Add global attributes */
            for (String key : allAttributes.keySet()) {
                ncds.addAttribute(null, new Attribute(key, allAttributes.get(key)));
            }
        } catch (IOException ex) {
            log.error("Error building trajectory NetcdfDataset", ex);
        } finally {
            if (ncds != null) {
                ncds.finish();
            }
        }

        return ncds;
    }

    public static NetcdfDataset buildTrajectoryProfile(IObservationGroup[] obsGroups) {
        throw new UnsupportedOperationException();
    }

    private static DataType getNcDataType(IObservationGroup.DataType obsDT) {
        switch (obsDT) {
            case INT:
                return DataType.INT;
            case LONG:
                return DataType.LONG;
            case FLOAT:
                return DataType.FLOAT;
            case DOUBLE:
            default:
                return DataType.DOUBLE;
        }
    }

    public static NetcdfDataset getNcdsFromTemplate(NcdsTemplate ncdsTemp) throws FileNotFoundException, IOException {
        File temp = File.createTempFile("ooi", ".ncml");
        temp.deleteOnExit();

        getSchemaTemplate(temp, ncdsTemp.getResourceName());
        return NetcdfDataset.openDataset(temp.getCanonicalPath());
    }

    private static void getSchemaTemplate(File tempFile, String schemaName) throws FileNotFoundException, IOException {
        java.io.InputStream in = null;
        java.io.FileOutputStream fos = null;
        try {
            ClassLoader cl = NcdsFactory.class.getClassLoader();
            in = cl.getResourceAsStream("resources/schemas/" + schemaName);
            fos = new java.io.FileOutputStream(tempFile);
            byte[] buff = new byte[1024];
            int len = 0;
            while ((len = in.read(buff)) > 0) {
                fos.write(buff, 0, len);
            }
        } finally {
            if (fos != null) {
                fos.close();
            }
            if (in != null) {
                in.close();
            }
        }
    }

    private static Array getNcArray(Number[] inarr, IObservationGroup.DataType dataType) {
        Array ret = null;
        int len = inarr.length;
        Object oarr = null;
        switch (dataType) {
            case INT:
                oarr = new int[len];
                for (int i = 0; i < len; i++) {
                    ((int[]) oarr)[i] = inarr[i].intValue();
                }
                break;
            case LONG:
                oarr = new long[len];
                for (int i = 0; i < len; i++) {
                    ((long[]) oarr)[i] = inarr[i].longValue();
                }
                break;
            case FLOAT:
                oarr = new float[len];
                for (int i = 0; i < len; i++) {
                    ((float[]) oarr)[i] = inarr[i].floatValue();
                }
                break;
            case DOUBLE:
                oarr = new double[len];
                for (int i = 0; i < len; i++) {
                    ((double[]) oarr)[i] = inarr[i].doubleValue();
                }
                break;
        }
        if (oarr != null) {
            ret = Array.factory(oarr);
        }
        return ret;
    }

    private static Array getNcScalar(Number inNum, IObservationGroup.DataType dataType) {
        Array ret = null;
        switch (dataType) {
            case INT:
                ret = new ArrayInt.D0();
                ((ArrayInt.D0) ret).set(inNum.intValue());
                break;
            case LONG:
                ret = new ArrayLong.D0();
                ((ArrayLong.D0) ret).set(inNum.longValue());
                break;
            case FLOAT:
                ret = new ArrayFloat.D0();
                ((ArrayFloat.D0) ret).set(inNum.floatValue());
                break;
            case DOUBLE:
                ret = new ArrayDouble.D0();
                ((ArrayDouble.D0) ret).set(inNum.doubleValue());
                break;
        }
        return ret;
    }

    private static void putArrayData(IndexIterator ii, DataType dataType, Number data) {
        switch (dataType) {
            case INT:
                ii.setIntNext(data.intValue());
                break;
            case LONG:
                ii.setLongNext(data.longValue());
                break;
            case FLOAT:
                ii.setFloatNext(data.floatValue());
                break;
            case DOUBLE:
                ii.setDoubleNext(data.doubleValue());
                break;
        }
    }

    public static void main(String[] args) {
        IObservationGroup og = new ObservationGroupImpl(0, "testStation", 41.82f, -71.21f);

        VariableParams swatts = new VariableParams(VariableParams.SEA_WATER_TEMPERATURE, IObservationGroup.DataType.FLOAT);
        og.addObservation(0, 1.0, 22f, swatts);
        og.addObservation(1, 1.0, 21f, swatts);
        og.addObservation(2, 1.0, 20f, swatts);
        og.addObservation(3, 1.0, 19f, swatts);

        VariableParams salatts = new VariableParams(VariableParams.SEA_WATER_SALINITY, IObservationGroup.DataType.INT);
        og.addObservation(0, 1.0, 22, salatts);
        og.addObservation(1, 1.0, 21, salatts);
        og.addObservation(2, 1.0, 20, salatts);
        og.addObservation(3, 1.0, 19, salatts);

        NetcdfDataset ncds = NcdsFactory.buildStation(og);
        System.out.println(ncds);
        try {
            ucar.nc2.FileWriter.writeToFile(ncds, "output/test/out1.nc");
        } catch (IOException ex) {
            log.error("Error writing NetCDF File", ex);
        }


        IObservationGroup og2 = new ObservationGroupImpl(0, "testStationProfile", 41.82f, -71.21f);

        VariableParams swatts2 = new VariableParams(VariableParams.SEA_WATER_TEMPERATURE, IObservationGroup.DataType.FLOAT);
        og2.addObservation(0, 1.0, 22f, swatts2);
        og2.addObservation(0, 2.0, 21f, swatts2);
        og2.addObservation(1, 1.0, 20f, swatts2);
        og2.addObservation(1, 2.0, 19f, swatts2);

        VariableParams salatts2 = new VariableParams(VariableParams.SEA_WATER_SALINITY, IObservationGroup.DataType.INT);
        og2.addObservation(0, 1.0, 22, salatts2);
        og2.addObservation(0, 2.0, 21, salatts2);
        og2.addObservation(1, 1.0, 20, salatts2);
        og2.addObservation(1, 2.0, 19, salatts2);

        NetcdfDataset ncds2 = NcdsFactory.buildStation(og2);
        System.out.println(ncds2);
        try {
            ucar.nc2.FileWriter.writeToFile(ncds, "output/test/out2.nc");
        } catch (IOException ex) {
            log.error("Error writing NetCDF File", ex);
        }


    }
}
