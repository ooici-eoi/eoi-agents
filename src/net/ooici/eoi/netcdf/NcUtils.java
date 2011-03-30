/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package net.ooici.eoi.netcdf;

import ucar.nc2.dataset.NetcdfDataset;

/**
 * Utility class containing useful methods involving Netcdf Java objects
 * 
 * @author cmueller
 */
public class NcUtils {

    private NcUtils() {
    }

    public static long estimateSize(ucar.nc2.dataset.NetcdfDataset ncd, java.util.HashMap<String, ucar.ma2.Range> ranges) {
        long size = 0;
        for (ucar.nc2.Variable v : ncd.getVariables()) {
            size += estimateSize(v, ranges, false);
        }
        return size;
    }

    /**
     * Estimates the filesize of the given NetcdfDataset.  Delegates to {@link estimateSize(NetcdfDataset ncd, boolean verbose) } with verbose == true
     * @param ncd a NetcdfDataset
     * @return the estimated file size of the NC Dataset in bytes
     */
    public static long estimateSize(ucar.nc2.dataset.NetcdfDataset ncd) {
        return estimateSize(ncd, false);
    }

    /**
     * Estimates the filesize of the given NetcdfDataset
     * @param ncd a NetcdfDataset
     * @param verbose print information about the variables "name: element_size x num_elements"
     * @return the estimated file size of the NC Dataset in bytes
     */
    public static long estimateSize(ucar.nc2.dataset.NetcdfDataset ncd, boolean verbose) {
        long size = 0;
        for (ucar.nc2.Variable v : ncd.getVariables()) {
            size += estimateSize(v, verbose);
        }
        return size;
    }

    /**
     * Estimates the size of a Netcdf Variable.  Delegates to {@link estimateSize(Variable v, boolean verbose) } with verbose == true
     * @param v the Variable to estimate size from
     * @return the estimated size of the variable in bytes
     */
    public static long estimateSize(ucar.nc2.Variable v) {
        return estimateSize(v, false);
    }

    /**
     * Estimates the size of a Netcdf Variable.
     * @param v the Variable to estimate size from
     * @param verbose print information about the variable "name: element_size x num_elements"
     * @return the estimated size of the variable in bytes
     */
    public static long estimateSize(ucar.nc2.Variable v, boolean verbose) {
        return estimateSize(v, null, verbose);
    }

    public static long estimateSize(ucar.nc2.Variable v, java.util.HashMap<String, ucar.ma2.Range> ranges, boolean verbose) {
        ucar.ma2.Section sec = getSubRangedSection(v, ranges);
        long sz = sec.computeSize();
        if (verbose) {
            System.out.println("\t" + v.getName() + ": " + v.getElementSize() + " x " + sz);
        }
        return v.getElementSize() * sz;
    }

    public static ucar.ma2.Section getSubRangedSection(ucar.nc2.Variable v, java.util.HashMap<String, ucar.ma2.Range> ranges) {
        ucar.ma2.Section sec = new ucar.ma2.Section(v.getShapeAsSection());
        if (ranges != null) {
            /* Apply any subRanges */
            for (int i = 0; i < sec.getRanges().size(); i++) {
                ucar.ma2.Range r = sec.getRange(i);
                if (ranges.containsKey(r.getName())) {
                    sec.replaceRange(i, ranges.get(r.getName()));
                }
            }
        }
        return sec;
    }

    /**
     * Attempts to determine the feature type of the given NetcdfDataset by analyizing readily available metadata
     * @param ncd A NetcdfDataset
     * @return a FeatureType value
     */
    public static ucar.nc2.constants.FeatureType determineFeatureType(ucar.nc2.dataset.NetcdfDataset ncd) {
        /* Check for the feature type using "helper" metadata (cdm_data_type, cdm_datatype, or thredds_data_type attribute) */
        ucar.nc2.constants.FeatureType ret = ucar.nc2.ft.FeatureDatasetFactoryManager.findFeatureType(ncd);
        if (ret != null) {
//            System.out.print("via FDFM.findFeatureType() --> ");
            return ret;
        }
        /* Try to open the dataset through the FactoryManager (more thorough check than above) */
        java.util.Formatter out = new java.util.Formatter();
        ucar.nc2.ft.FeatureDataset fds = null;
        try {
            fds = ucar.nc2.ft.FeatureDatasetFactoryManager.wrap(ucar.nc2.constants.FeatureType.ANY, ncd, null, out);
            if (fds != null) {
//                System.out.print("via FDFM.wrap() --> ");
                return fds.getFeatureType();
            }
        } catch (IllegalStateException ex) {
        } catch (java.io.IOException ex) {
        } finally {
            /** DO NOT close the FeatureDataset - this also closes the underlying ncd (passed) **/
//            if (fds != null) {
//                try {
//                    fds.close();
//                } catch (java.io.IOException ex) {
//                }
//            }
        }
        return ret;
    }

    /**
     * Determines if 2 {@link ucar.nc2.NetcdfDataset} objects are equivalent.  This is a "superficial"
     * check that does NOT check the data within the datasets.<br>
     * <b>NOTE: It assumes that the dimensions/attributes/variables are in the same order within the two files!!</b>
     * <p>
     * Checks performed are:<br>
     * <ul>
     * <li> Number of Dimensions are the same
     * <li> The name and length of each Dimension are equal
     * <li> The number of Variables are the same
     * <li> The name, datatype, length and Attributes of each Variable are equal - Attributes are checked as below
     * <li> The number of Attributes are the same
     * <li> The name, datatype, and value of each Attribute are equal
     * </ul>
     * @param ncds1
     * @param ncds2
     * @return
     */
    public static boolean checkEqual(NetcdfDataset ncds1, NetcdfDataset ncds2) {
        /* Check dimensions - do NOT check unlimitedness because returned datasets are always NOT unlimited */
        if (ncds1.getDimensions().size() == ncds2.getDimensions().size()) {
            ucar.nc2.Dimension dim1, dim2;
            for (int i = 0; i < ncds1.getDimensions().size(); i++) {
                dim1 = ncds1.getDimensions().get(i);
                dim2 = ncds2.getDimensions().get(i);
                if (!dim1.getName().equals(dim2.getName()) | dim1.getLength() != dim2.getLength()) {
                    return false;
                }
            }
        } else {
            return false;
        }

        /* Check variables, including attributes, but NOT data... */
        if (ncds1.getVariables().size() == ncds2.getVariables().size()) {
            ucar.nc2.Variable var1, var2;
            for (int i = 0; i < ncds1.getVariables().size(); i++) {
                var1 = ncds1.getVariables().get(i);
                var2 = ncds2.getVariables().get(i);
                if (!var1.getName().equals(var2.getName())) {
                    return false;
                }
                if (var1.getSize() != var2.getSize()) {
                    return false;
                }
                if (var1.getDataType() != var2.getDataType()) {
                    return false;
                }
                if (var1.getAttributes().size() == var1.getAttributes().size()) {
                    ucar.nc2.Attribute att1, att2;
                    for (int j = 0; j < var1.getAttributes().size(); j++) {
                        att1 = var1.getAttributes().get(j);
                        att2 = var2.getAttributes().get(j);
                        if (!att1.getName().equals(att2.getName())) {
                            return false;
                        }
                        if (att1.getLength() != att2.getLength()) {
                            return false;
                        }
                        if (att1.getDataType() != att2.getDataType()) {
                            return false;
                        }
                    }
                } else {
                    return false;
                }
            }
        } else {
            return false;
        }

        /* Check global attributes */
        if (ncds1.getGlobalAttributes().size() == ncds1.getGlobalAttributes().size()) {
            ucar.nc2.Attribute att1, att2;
            for (int j = 0; j < ncds1.getGlobalAttributes().size(); j++) {
                att1 = ncds1.getGlobalAttributes().get(j);
                att2 = ncds2.getGlobalAttributes().get(j);
                if (!att1.getName().equals(att2.getName())) {
                    return false;
                }
                if (att1.getLength() != att2.getLength()) {
                    return false;
                }
                if (att1.getDataType() != att2.getDataType()) {
                    return false;
                }
            }
        } else {
            return false;
        }


        return true;
    }
}
