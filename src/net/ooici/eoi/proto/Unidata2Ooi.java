/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package net.ooici.eoi.proto;

import ion.core.IonBootstrap;
import ion.core.utils.GPBWrapper;
import ion.core.utils.ProtoUtils;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.ooici.core.container.Container;
import net.ooici.data.cdm.Cdmdataset;
import net.ooici.cdm.syntactic.Cdmarray;
import net.ooici.cdm.syntactic.Cdmattribute;
import net.ooici.cdm.syntactic.Cdmdimension;
import net.ooici.cdm.syntactic.Cdmgroup;
import net.ooici.cdm.syntactic.Cdmvariable;
import net.ooici.eoi.datasetagent.AgentUtils;
import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.ma2.IndexIterator;
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
        
        packDataset(dataset, includeData);
        Container.Structure struct = structBldr.build();

        return struct.toByteArray();
    }

    public static byte[] varToByteArray(Variable var) throws IOException {
        /* Initialize the Structure Builder */
        structBldr = Container.Structure.newBuilder();

        /* Process the Unidata Variable */
        GPBWrapper<Cdmvariable.Variable> varWrap = getOoiVariable(var);
        ProtoUtils.addStructureElementToStructureBuilder(structBldr, varWrap.getStructureElement());

        /* Build the Structure object */
        Container.Structure struct = structBldr.build();
        return struct.toByteArray();
    }

    public static void packDataset(NetcdfDataset ncds) throws java.io.IOException {
        packDataset(ncds, true);
    }

    public static void packDataset(NetcdfDataset ncds, boolean includeData) throws java.io.IOException {
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
            GPBWrapper<Cdmvariable.Variable> varWrap = getOoiVariable(ncVar, includeData);
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
        if(arrWrap != null) {
            ProtoUtils.addStructureElementToStructureBuilder(structBldr, arrWrap.getStructureElement());
            attBldr.setArray(arrWrap.getCASRef());
        }

        return GPBWrapper.Factory(attBldr.build());
    }

    private static GPBWrapper<Cdmvariable.Variable> getOoiVariable(Variable ncVar) throws java.io.IOException {
        return getOoiVariable(ncVar, true);
    }

    private static GPBWrapper<Cdmvariable.Variable> getOoiVariable(Variable ncVar, boolean includeData) throws java.io.IOException {
        DataType dt = ncVar.getDataType();
        Cdmvariable.Variable.Builder varBldr = Cdmvariable.Variable.newBuilder().setName(ncVar.getName()).setDataType(AgentUtils.getOoiDataType(dt));

        /* Add all the attributes */
        for (Attribute ncAtt : ncVar.getAttributes()) {
            GPBWrapper<Cdmattribute.Attribute> attWrap = getOoiAttribute(ncAtt);
            ProtoUtils.addStructureElementToStructureBuilder(structBldr, attWrap.getStructureElement());
            varBldr.addAttributes(attWrap.getCASRef());
        }

        /* Set the shape - set of dimensions, not the nc-java "shape"... */
        /* TODO: may be able to trim some time by retrieving the dimension from the structure */
        for (Dimension ncDim : ncVar.getDimensions()) {
            GPBWrapper<Cdmdimension.Dimension> dimWrap = getOoiDimension(ncDim);
            ProtoUtils.addStructureElementToStructureBuilder(structBldr, dimWrap.getStructureElement());
            varBldr.addShape(dimWrap.getCASRef());
            /* We do NOT need to add the dimension to the structure because it's already there...*/
        }

        if (includeData) {
            /* Set the content */
            Cdmvariable.BoundedArray.Builder ooiBABldr = Cdmvariable.BoundedArray.newBuilder();
            Cdmvariable.BoundedArray.Bounds bnds;
            for (int i : ncVar.getShape()) {
                bnds = Cdmvariable.BoundedArray.Bounds.newBuilder().setOrigin(0).setSize(i).build();
                ooiBABldr.addBounds(bnds);
            }
            GPBWrapper arrWrap;
            Array ncArr;
            IndexIterator arrIter;
            switch (dt) {
                case BYTE:
                case SHORT:
                case INT:
                    Cdmarray.int32Array.Builder i32Bldr = Cdmarray.int32Array.newBuilder();
                    ncArr = ncVar.read();
                    arrIter = ncArr.getIndexIterator();
                    while (arrIter.hasNext()) {
                        i32Bldr.addValue(arrIter.getIntNext());
                    }
                    arrWrap = GPBWrapper.Factory(i32Bldr.build());
                    break;
                case LONG:
                    Cdmarray.int64Array.Builder i64Bldr = Cdmarray.int64Array.newBuilder();
                    ncArr = ncVar.read();
                    arrIter = ncArr.getIndexIterator();
                    while(arrIter.hasNext()) {
                        i64Bldr.addValue(arrIter.getLongNext());
                    }
                    arrWrap = GPBWrapper.Factory(i64Bldr.build());
                    break;
                case FLOAT:
                    Cdmarray.f32Array.Builder f32Bldr = Cdmarray.f32Array.newBuilder();
                    ncArr = ncVar.read();
                    arrIter = ncArr.getIndexIterator();
                    while(arrIter.hasNext()) {
                        f32Bldr.addValue(arrIter.getLongNext());
                    }
                    arrWrap = GPBWrapper.Factory(f32Bldr.build());
                    break;
                case DOUBLE:
                    Cdmarray.f64Array.Builder f64Bldr = Cdmarray.f64Array.newBuilder();
                    ncArr = ncVar.read();
                    arrIter = ncArr.getIndexIterator();
                    while(arrIter.hasNext()) {
                        f64Bldr.addValue(arrIter.getLongNext());
                    }
                    arrWrap = GPBWrapper.Factory(f64Bldr.build());
                    break;
                /* TODO: Implement other datatypes */

                default:
                    arrWrap = null;
            }
            if(arrWrap != null) {
                ProtoUtils.addStructureElementToStructureBuilder(structBldr, arrWrap.getStructureElement());
                ooiBABldr.setNdarray(arrWrap.getCASRef());
            }

            GPBWrapper<Cdmvariable.BoundedArray> bndArr = GPBWrapper.Factory(ooiBABldr.build());
            ProtoUtils.addStructureElementToStructureBuilder(structBldr, bndArr.getStructureElement());
            varBldr.addContent(bndArr.getCASRef());
        }

        return GPBWrapper.Factory(varBldr.build());
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

    private static void printBytes(byte[] key) {
        for (byte b : key) {
            System.out.print(" " + b);
//            System.out.print(SHA1.bytesToHex(b));

        }
        System.out.println();
    }
}
