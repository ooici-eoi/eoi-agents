/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package net.ooici.eoi.proto;

import com.google.protobuf.ByteString;
import ion.core.IonBootstrap;
import ion.core.utils.ProtoUtils;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.ooici.core.container.Container;
import net.ooici.core.type.Type;
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

    private static Container.Structure.Builder structBldr;

    public static byte[] ncdfToByteArray(NetcdfDataset dataset) throws IOException {
        return ncdfToByteArray(dataset, true);
    }

    public static byte[] ncdfToByteArray(NetcdfDataset dataset, boolean includeData) throws IOException {
        structBldr = Container.Structure.newBuilder();
        packDataset(dataset, includeData);
        Container.Structure struct = structBldr.build();

        return struct.toByteArray();
    }

    public static byte[] varToByteArray(Variable var) throws IOException {
        /* Initialize the Structure Builder */
        structBldr = Container.Structure.newBuilder();

        /* Process the Unidata Variable */
        Cdmvariable.Variable ooiVar = getOoiVariable(var);
        Type.GPBType gpbType = ProtoUtils.getGPBType(ooiVar.getClass());
        ByteString byteString = ooiVar.toByteString();
        ooiVar = null;
        byte[] key = ProtoUtils.getObjectKey(byteString, gpbType);
        addElementToStructure(false, key, gpbType, byteString);

        /* Build the Structure object */
        Container.Structure struct = structBldr.build();
        return struct.toByteArray();
    }

    public static void packDataset(NetcdfDataset ncds) throws java.io.IOException {
        packDataset(ncds, true);
    }

    public static void packDataset(NetcdfDataset ncds, boolean includeData) throws java.io.IOException {
        Type.GPBType gpbType;
        ByteString byteString;
        byte[] key;

        Cdmgroup.Group.Builder grpBldr = Cdmgroup.Group.newBuilder().setName("root");

        /* Add all of the Dimensions to the structure */
        for (Dimension ncDim : ncds.getDimensions()) {
            Cdmdimension.Dimension ooiDim = getOoiDimension(ncDim);
            gpbType = ProtoUtils.getGPBType(ooiDim.getClass());
            byteString = ooiDim.toByteString();
            ooiDim = null;
            key = ProtoUtils.getObjectKey(byteString, gpbType);
            addElementToStructure(true, key, gpbType, byteString);
            grpBldr.addDimensions(ProtoUtils.getLink(true, key, gpbType));
        }

        /* Add all of the Variables to the structure */
        for (Variable ncVar : ncds.getVariables()) {
            Cdmvariable.Variable ooiVar = getOoiVariable(ncVar, includeData);
            gpbType = ProtoUtils.getGPBType(ooiVar.getClass());
            byteString = ooiVar.toByteString();
            ooiVar = null;
            key = ProtoUtils.getObjectKey(byteString, gpbType);
            addElementToStructure(false, key, gpbType, byteString);
            grpBldr.addVariables(ProtoUtils.getLink(false, key, gpbType));
        }

        /* Add all of the Global Attributes to the structure */
        for (Attribute ncAtt : ncds.getGlobalAttributes()) {
            Cdmattribute.Attribute ooiAtt = getOoiAttribute(ncAtt);
            gpbType = ProtoUtils.getGPBType(ooiAtt.getClass());
            byteString = ooiAtt.toByteString();
            ooiAtt = null;
            key = ProtoUtils.getObjectKey(byteString, gpbType);
            addElementToStructure(false, key, gpbType, byteString);
            grpBldr.addAttributes(ProtoUtils.getLink(false, key, gpbType));
        }

        /* Build the group and add it to the structure */
        Cdmgroup.Group mainGroup = grpBldr.build();
        gpbType = ProtoUtils.getGPBType(mainGroup.getClass());
        byteString = mainGroup.toByteString();
        mainGroup = null;
        key = ProtoUtils.getObjectKey(byteString, gpbType);
        addElementToStructure(false, key, gpbType, byteString);

        /* Add the root group to the dataset - set the dataset as the head of the structure */
        Cdmdataset.Dataset dataset = Cdmdataset.Dataset.newBuilder().setRootGroup(ProtoUtils.getLink(false, key, gpbType)).build();
        gpbType = ProtoUtils.getGPBType(dataset.getClass());
        byteString = dataset.toByteString();
        dataset = null;
        key = ProtoUtils.getObjectKey(byteString, gpbType);
        structBldr.addHeads(Container.StructureElement.newBuilder().setIsleaf(false).setKey(com.google.protobuf.ByteString.copyFrom(key)).setType(gpbType).setValue(byteString).build());

        /* DONE!! */
    }

    private static void addElementToStructure(boolean isLeaf, byte[] key, Type.GPBType gpbType, ByteString value) {
        structBldr.addItems(ProtoUtils.getStructureElement(isLeaf, key, gpbType, value));
    }

    private static Cdmdimension.Dimension getOoiDimension(Dimension ncDim) {
        return Cdmdimension.Dimension.newBuilder().setName(ncDim.getName()).setLength(ncDim.getLength()).build();
    }

    private static Cdmattribute.Attribute getOoiAttribute(Attribute ncAtt) {

        DataType dt = ncAtt.getDataType();
        Cdmattribute.Attribute.Builder attBldr = Cdmattribute.Attribute.newBuilder().setName(ncAtt.getName()).setDataType(AgentUtils.getOoiDataType(dt));
        Type.GPBType gpbType = null;
        ByteString byteString = null;
        byte[] key;
        switch (dt) {
            case STRING:
                String val = ncAtt.getStringValue();
                Cdmarray.stringArray sarr = Cdmarray.stringArray.newBuilder().addValue(val).build();
                gpbType = ProtoUtils.getGPBType(sarr.getClass());
                byteString = sarr.toByteString();
                sarr = null;
                break;
            case BYTE:
            case SHORT:
            case INT:
                int i32Val = ncAtt.getNumericValue().intValue();
                Cdmarray.int32Array i32Arr = Cdmarray.int32Array.newBuilder().addValue(i32Val).build();
                gpbType = ProtoUtils.getGPBType(i32Arr.getClass());
                byteString = i32Arr.toByteString();
                i32Arr = null;
                break;
            case LONG:
                long i64Val = ncAtt.getNumericValue().longValue();
                Cdmarray.int64Array i64Arr = Cdmarray.int64Array.newBuilder().addValue(i64Val).build();
                gpbType = ProtoUtils.getGPBType(i64Arr.getClass());
                byteString = i64Arr.toByteString();
                i64Arr = null;
                break;
            case FLOAT:
                float f32Val = ncAtt.getNumericValue().floatValue();
                Cdmarray.f32Array f32Arr = Cdmarray.f32Array.newBuilder().addValue(f32Val).build();
                gpbType = ProtoUtils.getGPBType(f32Arr.getClass());
                byteString = f32Arr.toByteString();
                f32Arr = null;
                break;
            case DOUBLE:
                double f64Val = ncAtt.getNumericValue().doubleValue();
                Cdmarray.f64Array f64Arr = Cdmarray.f64Array.newBuilder().addValue(f64Val).build();
                gpbType = ProtoUtils.getGPBType(f64Arr.getClass());
                byteString = f64Arr.toByteString();
                f64Arr = null;
                break;
            /* TODO: Implement other datatypes */
            default:
                byteString = null;
                gpbType = null;
                break;
        }
        if (byteString != null && gpbType != null) {
            key = ProtoUtils.getObjectKey(byteString, gpbType);
            attBldr.setArray(ProtoUtils.getLink(true, key, gpbType));
            addElementToStructure(true, key, gpbType, byteString);
        }

        return attBldr.build();
    }

    private static Cdmvariable.Variable getOoiVariable(Variable ncVar) throws java.io.IOException {
        return getOoiVariable(ncVar, true);
    }

    private static Cdmvariable.Variable getOoiVariable(Variable ncVar, boolean includeData) throws java.io.IOException {

        DataType dt = ncVar.getDataType();
        Cdmvariable.Variable.Builder varBldr = Cdmvariable.Variable.newBuilder().setName(ncVar.getName()).setDataType(AgentUtils.getOoiDataType(dt));
        Type.GPBType gpbType;
        ByteString byteString;
        byte[] key;
        /* Add all the attributes */
        for (Attribute ncAtt : ncVar.getAttributes()) {
            Cdmattribute.Attribute ooiAtt = getOoiAttribute(ncAtt);
            gpbType = ProtoUtils.getGPBType(ooiAtt.getClass());
            byteString = ooiAtt.toByteString();
            ooiAtt = null;
            key = ProtoUtils.getObjectKey(byteString, gpbType);
            varBldr.addAttributes(ProtoUtils.getLink(false, key, gpbType));
            addElementToStructure(false, key, gpbType, byteString);
        }
        /* Set the shape - set of dimensions, not the nc-java "shape"... */
        /* TODO: may be able to trim some time by retrieving the dimension from the structure */
        for (Dimension ncDim : ncVar.getDimensions()) {
            Cdmdimension.Dimension ooiDim = getOoiDimension(ncDim);
            gpbType = ProtoUtils.getGPBType(ooiDim.getClass());
            byteString = ooiDim.toByteString();
            ooiDim = null;
            key = ProtoUtils.getObjectKey(byteString, gpbType);
            varBldr.addShape(ProtoUtils.getLink(true, key, gpbType));
            /* We do NOT need to add the dimension to the structure because it's already there...*/
        }

        if (includeData) {
            /* Set the content */
            Cdmvariable.BoundedArray.Builder ooiBABldr = Cdmvariable.BoundedArray.newBuilder();
            Cdmvariable.BoundedArray.Bounds bnds;
            for (int i : ncVar.getShape()) {
                bnds = Cdmvariable.BoundedArray.Bounds.newBuilder().setOrigin(0).setSize(i).build();
                ooiBABldr.addBounds(bnds);
//            addElementToStructure(true, SHA1.getSHA1Hash(bnds.toByteArray()), getGPBType(bnds.getClass()), bnds.toByteString());
            }
            switch (dt) {
                case BYTE:
                case SHORT:
                case INT:
                    Cdmarray.int32Array.Builder i32Bldr = Cdmarray.int32Array.newBuilder();
                    Array ints = ncVar.read();
                    IndexIterator iter = ints.getIndexIterator();
                    while (iter.hasNext()) {
                        i32Bldr.addValue(iter.getIntNext());
                    }

                    Cdmarray.int32Array i32 = i32Bldr.build();
                    gpbType = ProtoUtils.getGPBType(i32.getClass());
                    byteString = i32.toByteString();
                    i32 = null;
                    break;
                case LONG:
                    Cdmarray.int64Array.Builder i64Bldr = Cdmarray.int64Array.newBuilder();
                    long[] lngs = (long[]) ncVar.read().get1DJavaArray(long.class);
                    for (long l : lngs) {
                        i64Bldr.addValue(l);
                    }

                    Cdmarray.int64Array i64 = i64Bldr.build();
                    gpbType = ProtoUtils.getGPBType(i64.getClass());
                    byteString = i64.toByteString();
                    i64 = null;
                    break;
                case FLOAT:
                    Cdmarray.f32Array.Builder f32Bldr = Cdmarray.f32Array.newBuilder();
                    float[] flts = (float[]) ncVar.read().get1DJavaArray(float.class);
                    for (float f : flts) {
                        f32Bldr.addValue(f);
                    }

                    Cdmarray.f32Array f32 = f32Bldr.build();
                    gpbType = ProtoUtils.getGPBType(f32.getClass());
                    byteString = f32.toByteString();
                    f32 = null;
                    break;
                case DOUBLE:
                    Cdmarray.f64Array.Builder f64Bldr = Cdmarray.f64Array.newBuilder();
                    double[] dbls = (double[]) ncVar.read().get1DJavaArray(double.class);
                    for (double d : dbls) {
                        f64Bldr.addValue(d);
                    }

                    Cdmarray.f64Array f64 = f64Bldr.build();
                    gpbType = ProtoUtils.getGPBType(f64.getClass());
                    byteString = f64.toByteString();
                    f64 = null;
                    break;
                /* TODO: Implement other datatypes */

                default:
                    byteString = null;
                    gpbType = null;
            }
            if (byteString != null && gpbType != null) {
                key = ProtoUtils.getObjectKey(byteString, gpbType);
                ooiBABldr.setNdarray(ProtoUtils.getLink(true, key, gpbType));
                addElementToStructure(true, key, gpbType, byteString);
            }
            Cdmvariable.BoundedArray bndArr = ooiBABldr.build();
            gpbType = ProtoUtils.getGPBType(bndArr.getClass());
            byteString = bndArr.toByteString();
            bndArr = null;
            key = ProtoUtils.getObjectKey(byteString, gpbType);
            varBldr.addContent(ProtoUtils.getLink(false, key, gpbType));
            addElementToStructure(false, key, gpbType, byteString);
        }

        return varBldr.build();
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
