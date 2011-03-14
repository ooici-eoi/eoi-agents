/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package net.ooici;

import java.util.Comparator;
import java.math.BigInteger;
import java.math.BigDecimal;

/**
 * Generic comparator for Number objects of different type.<br/>
 * <br/>
 * @author cmueller
 */
public class NumberComparator<E extends Number> implements Comparator<E> {

    @Override
    public int compare(E n1, E n2) {
        return compareNumbers(n1, n2);
    }

	public static <T extends Number> int compareNumbers(T n1, T n2) {
	    if (n1 == null || n2 == null)
            throw new IllegalArgumentException("comparing null value");
        if (n1 instanceof Integer)
            return ((Integer) n1).compareTo(n2.intValue());
        else if (n1 instanceof Long)
            return ((Long) n1).compareTo(n2.longValue());
        else if (n1 instanceof Short)
            return ((Short) n1).compareTo(n2.shortValue());
        else if (n1 instanceof Byte)
            return ((Byte) n1).compareTo(n2.byteValue());
        else if (n1 instanceof Float)
            return ((Float) n1).compareTo(n2.floatValue());
        else if (n1 instanceof Double)
            return ((Double) n1).compareTo(n2.doubleValue());
        else if (n1 instanceof BigInteger)
            return ((BigInteger) n1).compareTo((BigInteger) n2);
        else if (n1 instanceof BigDecimal)
            return ((BigDecimal) n1).compareTo((BigDecimal) n2);
        else
            throw new UnsupportedOperationException("Unsupported Number type: " + n1.getClass());
    }
}
