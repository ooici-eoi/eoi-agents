/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package net.ooici.eoi.proto;

import net.ooici.cdm.syntactic.Cdmdatatype;
import net.ooici.core.type.Type;
import ucar.ma2.DataType;

/**
 *
 * @author cmueller
 */
public class ProtoUtils {

    private ProtoUtils() {
    }
    
//
//    public static String getClassStringFromGPBType(Type.GPBType gpbType) {
//        StringBuilder sb = new StringBuilder(gpbType.getPackage());
//        sb.append(".").append(gpbType.getProtofile().substring(0, 1).toUpperCase()).append(gpbType.getProtofile().substring(1));
//        sb.append("$").append(gpbType.getCls());
//        return sb.toString();
//    }

    public static Cdmdatatype.DataType getOoiDataType(DataType ucarDT) {
        Cdmdatatype.DataType ret = null;

        switch (ucarDT) {
            case BOOLEAN:
                ret = Cdmdatatype.DataType.BOOLEAN;
                break;
            case BYTE:
                ret = Cdmdatatype.DataType.BYTE;
                break;
            case SHORT:
                ret = Cdmdatatype.DataType.SHORT;
                break;
            case INT:
                ret = Cdmdatatype.DataType.INT;
                break;
            case LONG:
                ret = Cdmdatatype.DataType.LONG;
                break;
            case FLOAT:
                ret = Cdmdatatype.DataType.FLOAT;
                break;
            case DOUBLE:
                ret = Cdmdatatype.DataType.DOUBLE;
                break;
            case CHAR:
                ret = Cdmdatatype.DataType.CHAR;
                break;
            case STRING:
                ret = Cdmdatatype.DataType.STRING;
                break;
            case STRUCTURE:
                ret = Cdmdatatype.DataType.STRUCTURE;
                break;
            case SEQUENCE:
                ret = Cdmdatatype.DataType.SEQUENCE;
                break;
            case ENUM1:
            case ENUM2:
            case ENUM4:
                ret = Cdmdatatype.DataType.ENUM;
                break;
            case OPAQUE:
                ret = Cdmdatatype.DataType.OPAQUE;
                break;
            default:
                ret = Cdmdatatype.DataType.STRING;
        }
        return ret;
    }

    public static DataType getNcDataType(Cdmdatatype.DataType ooiDT) {
        DataType ret = null;

        switch (ooiDT) {
            case BOOLEAN:
                ret = DataType.BOOLEAN;
                break;
            case BYTE:
                ret = DataType.BYTE;
                break;
            case SHORT:
                ret = DataType.SHORT;
                break;
            case INT:
                ret = DataType.INT;
                break;
            case LONG:
                ret = DataType.LONG;
                break;
            case FLOAT:
                ret = DataType.FLOAT;
                break;
            case DOUBLE:
                ret = DataType.DOUBLE;
                break;
            case CHAR:
                ret = DataType.CHAR;
                break;
            case STRING:
                ret = DataType.STRING;
                break;
            case STRUCTURE:
                ret = DataType.STRUCTURE;
                break;
            case SEQUENCE:
                ret = DataType.SEQUENCE;
                break;
            case ENUM:
                ret = DataType.ENUM1;
                break;
            case OPAQUE:
                ret = DataType.OPAQUE;
                break;
            default:
                ret = DataType.STRING;
        }
        return ret;
    }
}
