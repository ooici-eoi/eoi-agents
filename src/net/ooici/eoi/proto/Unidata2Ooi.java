/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package net.ooici.eoi.proto;

import ion.core.IonBootstrap;
import ion.core.utils.GPBWrapper;
import ion.core.utils.ProtoUtils;
import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.ooici.core.container.Container;
import net.ooici.data.cdm.Cdmdataset;
import net.ooici.cdm.syntactic.Cdmarray;
import net.ooici.cdm.syntactic.Cdmattribute;
import net.ooici.cdm.syntactic.Cdmdimension;
import net.ooici.cdm.syntactic.Cdmgroup;
import net.ooici.cdm.syntactic.Cdmvariable;
import net.ooici.core.link.Link.CASRef;
import net.ooici.eoi.datasetagent.AgentUtils;
import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.ma2.IndexIterator;
import ucar.ma2.InvalidRangeException;
import ucar.ma2.Range;
import ucar.ma2.Section;
import ucar.nc2.Attribute;
import ucar.nc2.Dimension;
import ucar.nc2.Variable;
import ucar.nc2.dataset.NetcdfDataset;

/**
 *
 * @author cmueller
 */
public class Unidata2Ooi {

    private static Container.Structure.Builder structBldr = null;

    public static byte[] ncdfToByteArray(NetcdfDataset dataset) throws IOException {
        return ncdfToByteArray(dataset, true);
    }

    public static byte[] ncdfToByteArray(NetcdfDataset dataset, boolean includeData) throws IOException {
        /* Initialize the Structure Builder */
        structBldr = Container.Structure.newBuilder();
        if (dataset != null) {
            packDataset(dataset, includeData);
        }
        Container.Structure struct = structBldr.build();
//        ion.core.utils.StructureManager sm = ion.core.utils.StructureManager.Factory(struct);
//        System.out.println(">>>>Struct<<<<");
//        System.out.println(sm.toString());

        return struct.toByteArray();
    }

//    public static byte[] varToByteArray(Variable var) throws IOException {
//        return varToByteArray(var, null);
//    }
//    public static byte[] varToByteArray(Variable var, Section section) throws IOException {
//        return varToStructure(var, section).toByteArray();
//    }
//    public static Container.Structure varToStructure(Variable var, Section section) throws IOException {
//        /* Initialize the Structure Builder */
//        structBldr = Container.Structure.newBuilder();
//
//        /* TODO: Set the head with the dataset ID */
//
//
//        /* Process the Unidata Variable */
//        GPBWrapper<Cdmvariable.Variable> varWrap = getOoiVariable(var, section);
//        ProtoUtils.addStructureElementToStructureBuilder(structBldr, varWrap.getStructureElement());
//
//        /* Build and return the Structure object */
//        return structBldr.build();
//    }
    private static void packDataset(NetcdfDataset ncds) throws java.io.IOException {
        packDataset(ncds, true);
    }

    private static void packDataset(NetcdfDataset ncds, boolean includeData) throws java.io.IOException {
        /* Instantiate the Root Group builder */
        Cdmgroup.Group.Builder grpBldr = Cdmgroup.Group.newBuilder().setName("root");

        /* Add all of the Dimensions to the structure */
        for (Dimension ncDim : ncds.getDimensions()) {
            GPBWrapper<Cdmdimension.Dimension> dimWrap = getOoiDimension(ncDim);
            ProtoUtils.addStructureElementToStructureBuilder(structBldr, dimWrap.getStructureElement());
            grpBldr.addDimensions(dimWrap.getCASRef());
        }

        /* Add all of the Variables to the structure */
        for (Variable ncVar : ncds.getVariables()) {
            GPBWrapper<Cdmvariable.Variable> varWrap = getOoiVariable(ncVar, null, includeData);
            ProtoUtils.addStructureElementToStructureBuilder(structBldr, varWrap.getStructureElement());
            grpBldr.addVariables(varWrap.getCASRef());
        }

        /* Add all of the Global Attributes to the structure */
        for (Attribute ncAtt : ncds.getGlobalAttributes()) {
            GPBWrapper<Cdmattribute.Attribute> attWrap = getOoiAttribute(ncAtt);
            ProtoUtils.addStructureElementToStructureBuilder(structBldr, attWrap.getStructureElement());
            grpBldr.addAttributes(attWrap.getCASRef());
        }

        /* Build the group and add it to the structure */
        GPBWrapper<Cdmgroup.Group> grpWrap = GPBWrapper.Factory(grpBldr.build());
        ProtoUtils.addStructureElementToStructureBuilder(structBldr, grpWrap.getStructureElement());

        /* Add the root group to the dataset - set the dataset as the head of the structure */
        GPBWrapper<Cdmdataset.Dataset> dsWrap = GPBWrapper.Factory(Cdmdataset.Dataset.newBuilder().setRootGroup(grpWrap.getCASRef()).build());
        ProtoUtils.addStructureElementToStructureBuilder(structBldr, dsWrap.getStructureElement(), true);

        /* DONE!! */
    }

    private static GPBWrapper<Cdmdimension.Dimension> getOoiDimension(Dimension ncDim) {
        return GPBWrapper.Factory(Cdmdimension.Dimension.newBuilder().setName(ncDim.getName()).setLength(ncDim.getLength()).build());
    }

    private static GPBWrapper<Cdmattribute.Attribute> getOoiAttribute(Attribute ncAtt) {

        DataType dt = ncAtt.getDataType();
        Cdmattribute.Attribute.Builder attBldr = Cdmattribute.Attribute.newBuilder().setName(ncAtt.getName()).setDataType(AgentUtils.getOoiDataType(dt));
        GPBWrapper arrWrap;
        switch (dt) {
            case STRING:
                String val = ncAtt.getStringValue();
                arrWrap = GPBWrapper.Factory(Cdmarray.stringArray.newBuilder().addValue(val).build());
                break;
            case BYTE:
            case SHORT:
            case INT:
                int i32Val = ncAtt.getNumericValue().intValue();
                arrWrap = GPBWrapper.Factory(Cdmarray.int32Array.newBuilder().addValue(i32Val).build());
                break;
            case LONG:
                long i64Val = ncAtt.getNumericValue().longValue();
                arrWrap = GPBWrapper.Factory(Cdmarray.int64Array.newBuilder().addValue(i64Val).build());
                break;
            case FLOAT:
                float f32Val = ncAtt.getNumericValue().floatValue();
                arrWrap = GPBWrapper.Factory(Cdmarray.f32Array.newBuilder().addValue(f32Val).build());
                break;
            case DOUBLE:
                double f64Val = ncAtt.getNumericValue().doubleValue();
                arrWrap = GPBWrapper.Factory(Cdmarray.f64Array.newBuilder().addValue(f64Val).build());
                break;
            /* TODO: Implement other datatypes */
            default:
                arrWrap = null;
                break;
        }
        if (arrWrap != null) {
            ProtoUtils.addStructureElementToStructureBuilder(structBldr, arrWrap.getStructureElement());
            attBldr.setArray(arrWrap.getCASRef());
        }

        return GPBWrapper.Factory(attBldr.build());
    }

//    private static GPBWrapper<Cdmvariable.Variable> getOoiVariable(Variable ncVar) throws java.io.IOException {
//        return getOoiVariable(ncVar, true);
//    }
//
//    private static GPBWrapper<Cdmvariable.Variable> getOoiVariable(Variable ncVar, boolean includeData) throws java.io.IOException {
//        return getOoiVariable(ncVar, null, includeData);
//    }
//    private static GPBWrapper<Cdmvariable.Variable> getOoiVariable(Variable ncVar, Section section) throws java.io.IOException {
//        return getOoiVariable(ncVar, section, true);
//    }
    private static GPBWrapper<Cdmvariable.Variable> getOoiVariable(Variable ncVar, Section section, boolean includeData) throws java.io.IOException {
        /* If section is null, section is all available data */
        section = (section == null) ? ncVar.getShapeAsSection() : section;

        DataType dt = ncVar.getDataType();
        Cdmvariable.Variable.Builder varBldr = Cdmvariable.Variable.newBuilder().setName(ncVar.getName()).setDataType(AgentUtils.getOoiDataType(dt));

        /* Add all the attributes */
        for (Attribute ncAtt : ncVar.getAttributes()) {
            GPBWrapper<Cdmattribute.Attribute> attWrap = getOoiAttribute(ncAtt);
            ProtoUtils.addStructureElementToStructureBuilder(structBldr, attWrap.getStructureElement());
            varBldr.addAttributes(attWrap.getCASRef());
        }

        /* Set the shape - set of dimensions, not the nc-java "shape"... */
        for (Dimension ncDim : ncVar.getDimensions()) {
            GPBWrapper<Cdmdimension.Dimension> dimWrap = getOoiDimension(ncDim);
            ProtoUtils.addStructureElementToStructureBuilder(structBldr, dimWrap.getStructureElement());
            varBldr.addShape(dimWrap.getCASRef());
        }

        Cdmvariable.BoundedArray bndArr = null;
        if (includeData) {
            /* Set the content */
            /* Build the array and the bounded array*/
            GPBWrapper arrWrap = getOoiArray(ncVar, section);
            if (arrWrap != null) {
                ProtoUtils.addStructureElementToStructureBuilder(structBldr, arrWrap.getStructureElement());
                bndArr = getBoundedArray(section, arrWrap.getCASRef());
            }
        } else {
            /* TODO: Ask David if this is right --> Build the bounded array for the section, but without an ndarray reference */
            bndArr = getBoundedArray(section, null);
        }

        GPBWrapper<Cdmvariable.BoundedArray> baWrap = GPBWrapper.Factory(bndArr);
        ProtoUtils.addStructureElementToStructureBuilder(structBldr, baWrap.getStructureElement());
        varBldr.addContent(baWrap.getCASRef());

        return GPBWrapper.Factory(varBldr.build());
    }

    public static Cdmvariable.BoundedArray getBoundedArray(ucar.ma2.Section section, CASRef arrRef) {
        /* No section == empty BA */
        if (section == null) {
            return Cdmvariable.BoundedArray.newBuilder().build();
        }

        return getBoundedArray(section.getRanges(), arrRef);
    }

    public static Cdmvariable.BoundedArray getBoundedArray(List<ucar.ma2.Range> ranges, CASRef arrRef) {
        /* No ranges == empty BA */
        if (ranges == null) {
            return Cdmvariable.BoundedArray.newBuilder().build();
        }

        Cdmvariable.BoundedArray.Builder baBldr = Cdmvariable.BoundedArray.newBuilder();
        Cdmvariable.BoundedArray.Bounds bnds;
        for (Range rng : ranges) {
            bnds = Cdmvariable.BoundedArray.Bounds.newBuilder().setOrigin(rng.first()).setSize(rng.length()).build();
            baBldr.addBounds(bnds);
        }
        if (arrRef != null) {
            baBldr.setNdarray(arrRef);
        }
        return baBldr.build();
    }

    public static GPBWrapper getOoiArray(Variable ncVar, Section section) throws IOException {
        /* If section is null, section is all available data */
        section = (section == null) ? ncVar.getShapeAsSection() : section;

        /* Get the datatype*/
        DataType dt = ncVar.getDataType();
        /* Ensure the BoundedArray builder is initialized */
//        ooiBABldr = (ooiBABldr == null) ? Cdmvariable.BoundedArray.newBuilder() : ooiBABldr;
        /* Set the content */

        Array ncArr = null;
        try {
            ncArr = ncVar.read(section);
        } catch (InvalidRangeException ex) {
            throw new IOException(ex);
        }
        IndexIterator arrIter = ncArr.getIndexIterator();

        GPBWrapper arrWrap = null;
        switch (dt) {
            case BYTE:
            case SHORT:
            case INT:
                Cdmarray.int32Array.Builder i32Bldr = Cdmarray.int32Array.newBuilder();
                while (arrIter.hasNext()) {
                    i32Bldr.addValue(arrIter.getIntNext());
                }
                arrWrap = GPBWrapper.Factory(i32Bldr.build());
                break;
            case LONG:
                Cdmarray.int64Array.Builder i64Bldr = Cdmarray.int64Array.newBuilder();
                while (arrIter.hasNext()) {
                    i64Bldr.addValue(arrIter.getLongNext());
                }
                arrWrap = GPBWrapper.Factory(i64Bldr.build());
                break;
            case FLOAT:
                Cdmarray.f32Array.Builder f32Bldr = Cdmarray.f32Array.newBuilder();
                while (arrIter.hasNext()) {
                    f32Bldr.addValue(arrIter.getFloatNext());
                }
                arrWrap = GPBWrapper.Factory(f32Bldr.build());
                break;
            case DOUBLE:
                Cdmarray.f64Array.Builder f64Bldr = Cdmarray.f64Array.newBuilder();
                while (arrIter.hasNext()) {
                    f64Bldr.addValue(arrIter.getDoubleNext());
                }
                arrWrap = GPBWrapper.Factory(f64Bldr.build());
                break;
            /* TODO: Implement other datatypes */

            default:
                arrWrap = null;
        }
        return arrWrap;
    }

    public static void main(String[] args) {
        try {
            IonBootstrap.bootstrap();

            String ds = "/Users/cmueller/Development/OOI/Dev/code/eoidev/proto_test/station_profile.nc";

            NetcdfDataset ncds = NetcdfDataset.openDataset(ds);
//            byte[] data = Unidata2Ooi.ncdfToByteArray(ncds);
//            System.out.println(data);

            structBldr = Container.Structure.newBuilder();


            packDataset(ncds);


            Container.Structure struct = structBldr.build();
            /* Print structure to console */
//        System.out.println("************ Structure ************");
//        System.out.println(struct);

            /* Write structure to disk */
            new java.io.File("output").mkdirs();
            java.io.FileOutputStream fos = new java.io.FileOutputStream("output/" + "" + ds.substring(ds.lastIndexOf("/")).replace(".nc", ".protostruct"));
            struct.writeTo(fos);

        } catch (Exception ex) {
            Logger.getLogger(Unidata2Ooi.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
