/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package net.ooici.eoi.netcdf;

import ion.core.IonException;
import java.io.IOException;
import java.util.HashMap;
import net.ooici.eoi.datasetagent.AgentUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.ma2.Array;
import ucar.ma2.MAMath;
import ucar.ma2.Range;
import ucar.nc2.Attribute;
import ucar.nc2.Variable;
import ucar.nc2.constants.AxisType;
import ucar.nc2.constants.CF;
import ucar.nc2.constants.FeatureType;
import ucar.nc2.dataset.CoordinateAxis;
import ucar.nc2.dataset.CoordinateAxis1DTime;
import ucar.nc2.dataset.CoordinateAxis2D;
import ucar.nc2.dataset.NetcdfDataset;
import ucar.units.Unit;
import ucar.units.UnitFormat;
import ucar.units.UnitFormatManager;

/**
 *
 * @author cmueller
 */
public class AttributeFactory {

    private static final Logger log = LoggerFactory.getLogger(AttributeFactory.class);

    private AttributeFactory() {
    }

    public static void addTimeBoundsMetadata(NetcdfDataset ncds) {
        addTimeBoundsMetadata(ncds, null);
    }

    public static void addTimeBoundsMetadata(NetcdfDataset ncds, HashMap<String, Range> subRanges) {
        String startdate = "";
        String enddate = "";
//        CoordinateAxis ca = ncds.findCoordinateAxis(AxisType.Time);
        CoordinateAxis ca = ncds.findCoordinateAxis(AxisType.RunTime);
        if(ca == null) {
            ca = ncds.findCoordinateAxis(AxisType.Time);
        }
        Throwable thrown = null;
        if (ca != null) {
//            if (ca instanceof CoordinateAxis2D) {
//                ca = ncds.findCoordinateAxis(AxisType.RunTime);
//            }
            CoordinateAxis1DTime cat = null;
            if (ca instanceof CoordinateAxis1DTime) {
                cat = (CoordinateAxis1DTime) ca;
            } else {
                try {
                    cat = CoordinateAxis1DTime.factory(ncds, new ucar.nc2.dataset.VariableDS(null, ncds.findVariable(ca.getName()), true), null);
                } catch (IOException ex) {
                    thrown = ex;
                }
            }
            if (cat != null) {
                int sti = 0;
                int eti = (int) cat.getSize() - 1;
                if (subRanges != null) {
                    Range r = subRanges.get(cat.getName());
                    if (r != null) {
                        sti = r.first();
                        eti = r.last();
                    }
                }

                startdate = AgentUtils.ISO8601_DATE_FORMAT.format(cat.getTimeDate(sti));
                enddate = AgentUtils.ISO8601_DATE_FORMAT.format(cat.getTimeDate(eti));
            }
        } else {
            thrown = new IonException("The dataset does not contain a valid temporal coordinate axis");
        }

        if(thrown != null) {
            throw new IonException("Unable to determine start/end metadata", thrown);
        }
        
        ncds.addAttribute(null, new Attribute(IonNcConstants.ION_TIME_COVERAGE_START, startdate));
        ncds.addAttribute(null, new Attribute(IonNcConstants.ION_TIME_COVERAGE_END, enddate));
    }

    public static void addLatBoundsMetadata(NetcdfDataset ncds, FeatureType ftype) {
        Number[] minMax = getMetadata(ncds, ftype, AxisType.Lat, "lat", "latitude");

        ncds.addAttribute(null, new Attribute(IonNcConstants.ION_GEOSPATIAL_LAT_MIN, minMax[0]));
        ncds.addAttribute(null, new Attribute(IonNcConstants.ION_GEOSPATIAL_LAT_MAX, minMax[1]));
    }

    public static void addLonBoundsMetadata(NetcdfDataset ncds, FeatureType ftype) {
        Number[] minMax = getMetadata(ncds, ftype, AxisType.Lon, "lon", "longitude");

        double maxcheck = minMax[1].doubleValue();
        if (maxcheck > 180) {
            minMax[1] = maxcheck - 360;
        }

        ncds.addAttribute(null, new Attribute(IonNcConstants.ION_GEOSPATIAL_LON_MIN, minMax[0]));
        ncds.addAttribute(null, new Attribute(IonNcConstants.ION_GEOSPATIAL_LON_MAX, minMax[1]));
    }

    public static void addVertBoundsMetadata(NetcdfDataset ncds, FeatureType ftype) {
        Number[] minMax = getMetadata(ncds, ftype, AxisType.Height, "z", "depth");

        boolean isNan = (Double.isNaN(minMax[0].doubleValue()) || Double.isNaN(minMax[1].floatValue()));

        /* Determine positive direction */
        String posDir = "";//default to ""

        for (Variable v : ncds.getVariables()) {
            Attribute a = v.findAttribute("positive");
            if (a == null) {
                a = v.findAttribute("_CoordinateZisPositive");
            }
            if (a != null) {
                posDir = a.getStringValue();
                /* If we don't have values, use the variable (is there another reason to have "positive"??) */
                if (isNan) {
                    try {
                        Array arr = v.read();
                        minMax[0] = MAMath.getMinimum(arr);
                        minMax[1] = MAMath.getMaximum(arr);
                        UnitFormat format = UnitFormatManager.instance();
                        Unit from = null;
                        Unit to = null;
                        try {
                            Attribute unit = v.findAttribute("units");
                            if (unit != null) {
                                from = format.parse(unit.getStringValue());
                                to = format.parse("m");

                                minMax[0] = from.convertTo(minMax[0].doubleValue(), to);
                                minMax[1] = from.convertTo(minMax[1].doubleValue(), to);
                            } else {
                                throw new Exception("No \"units\" attribute");
                            }
                        } catch (Exception e) {
                            throw new IOException("Can't convert/calculate vertical", e);
                        }

                    } catch (IOException ex) {
                        log.error("Error calculating min/max for vertical coordinate", ex);
                        minMax[0] = Double.NaN;
                        minMax[1] = Double.NaN;
                        // exit quietly, return nans
                        posDir = "";
                    }
                }
                break;
            }
        }

        ncds.addAttribute(null, new Attribute(IonNcConstants.ION_GEOSPATIAL_VERTICAL_MIN, minMax[0]));
        ncds.addAttribute(null, new Attribute(IonNcConstants.ION_GEOSPATIAL_VERTICAL_MAX, minMax[1]));
        ncds.addAttribute(null, new Attribute(IonNcConstants.ION_GEOSPATIAL_VERTICAL_POSITIVE, posDir));
    }

    private static Number[] getMetadata(NetcdfDataset ncds, FeatureType ftype, AxisType atype, String vname, String sname) {
        boolean isVert = (atype == AxisType.Height);

        if (null == ftype) {
            ftype = FeatureType.NONE;
        }


        Number min = Double.NaN;
        Number max = Double.NaN;
        CoordinateAxis ca = null;
        Variable var;
        ca = ncds.findCoordinateAxis(atype);
        if (ca != null) {
            max = ca.getMaxValue();
            min = ca.getMinValue();
            /* If the axis is vertical, attempt to convert to "m" */
            if (isVert) {
                UnitFormat format = UnitFormatManager.instance();
                Unit from = null;
                Unit to = null;
                try {
                    from = format.parse(ca.getUnitsString());
                    to = format.parse("m");

                    max = from.convertTo(max.doubleValue(), to);
                    min = from.convertTo(min.doubleValue(), to);
                } catch (Exception e) {
                    log.error("Error converting units of coordinate axis to meters: axis==" + ca, e);
                }
            }
        } else {
            /* Try to find the variable by "typical" variable name */
            var = ncds.findVariable(vname);
            if (var == null) {
                /* Look for it by standard_name */
                for (Variable v : ncds.getVariables()) {
                    Attribute a = v.findAttribute(CF.STANDARD_NAME);
                    if (a != null && a.getStringValue().equalsIgnoreCase(sname)) {
                        var = v;
                        continue;
                    }
                }
            }

            /* NOTE: falls through to 'default' if var is still null by this point */
            if (var != null) {
                try {
                    Array arr = var.read();
                    min = MAMath.getMinimum(arr);
                    max = MAMath.getMaximum(arr);
                } catch (IOException ex) {
                    /* fall through to default */
                    min = Double.NaN;
                    max = Double.NaN;
                }
            }
        }
        return new Number[]{min, max};
    }
}
