/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package net.ooici.eoi.proto;

/**
 *
 * @author cmueller
 */
public class ProtoUtils {

    private ProtoUtils() {
    }

    public static String getClassStringFromGPBType(net.ooici.core.type.Type.GPBType gpbType) {
        StringBuilder sb = new StringBuilder(gpbType.getPackage());
        sb.append(".").append(gpbType.getProtofile().substring(0, 1).toUpperCase()).append(gpbType.getProtofile().substring(1));
        sb.append("$").append(gpbType.getCls());
        return sb.toString();
    }

    public static net.ooici.data.cdm.syntactic.Cdmdatatype.DataType getOoiDataType(ucar.ma2.DataType ucarDT) {
        net.ooici.data.cdm.syntactic.Cdmdatatype.DataType ret = null;

        switch (ucarDT) {
            case BOOLEAN:
                ret = net.ooici.data.cdm.syntactic.Cdmdatatype.DataType.BOOLEAN;
                break;
            case BYTE:
                ret = net.ooici.data.cdm.syntactic.Cdmdatatype.DataType.BYTE;
                break;
            case SHORT:
                ret = net.ooici.data.cdm.syntactic.Cdmdatatype.DataType.SHORT;
                break;
            case INT:
                ret = net.ooici.data.cdm.syntactic.Cdmdatatype.DataType.INT;
                break;
            case LONG:
                ret = net.ooici.data.cdm.syntactic.Cdmdatatype.DataType.LONG;
                break;
            case FLOAT:
                ret = net.ooici.data.cdm.syntactic.Cdmdatatype.DataType.FLOAT;
                break;
            case DOUBLE:
                ret = net.ooici.data.cdm.syntactic.Cdmdatatype.DataType.DOUBLE;
                break;
            case CHAR:
                ret = net.ooici.data.cdm.syntactic.Cdmdatatype.DataType.CHAR;
                break;
            case STRING:
                ret = net.ooici.data.cdm.syntactic.Cdmdatatype.DataType.STRING;
                break;
            case STRUCTURE:
                ret = net.ooici.data.cdm.syntactic.Cdmdatatype.DataType.STRUCTURE;
                break;
            case SEQUENCE:
                ret = net.ooici.data.cdm.syntactic.Cdmdatatype.DataType.SEQUENCE;
                break;
            case ENUM1:
            case ENUM2:
            case ENUM4:
                ret = net.ooici.data.cdm.syntactic.Cdmdatatype.DataType.ENUM;
                break;
            case OPAQUE:
                ret = net.ooici.data.cdm.syntactic.Cdmdatatype.DataType.OPAQUE;
                break;
            default:
                ret = net.ooici.data.cdm.syntactic.Cdmdatatype.DataType.STRING;
        }
        return ret;
    }

    public static ucar.ma2.DataType getNcDataType(net.ooici.data.cdm.syntactic.Cdmdatatype.DataType ooiDT) {
        ucar.ma2.DataType ret = null;

        switch (ooiDT) {
            case BOOLEAN:
                ret = ucar.ma2.DataType.BOOLEAN;
                break;
            case BYTE:
                ret = ucar.ma2.DataType.BYTE;
                break;
            case SHORT:
                ret = ucar.ma2.DataType.SHORT;
                break;
            case INT:
                ret = ucar.ma2.DataType.INT;
                break;
            case LONG:
                ret = ucar.ma2.DataType.LONG;
                break;
            case FLOAT:
                ret = ucar.ma2.DataType.FLOAT;
                break;
            case DOUBLE:
                ret = ucar.ma2.DataType.DOUBLE;
                break;
            case CHAR:
                ret = ucar.ma2.DataType.CHAR;
                break;
            case STRING:
                ret = ucar.ma2.DataType.STRING;
                break;
            case STRUCTURE:
                ret = ucar.ma2.DataType.STRUCTURE;
                break;
            case SEQUENCE:
                ret = ucar.ma2.DataType.SEQUENCE;
                break;
            case ENUM:
                ret = ucar.ma2.DataType.ENUM1;
                break;
            case OPAQUE:
                ret = ucar.ma2.DataType.OPAQUE;
                break;
            default:
                ret = ucar.ma2.DataType.STRING;
        }
        return ret;
    }
}
