/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package net.ooici.eoi.datasetagent.obs;


import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.ooici.NumberComparator;

import net.ooici.Pair;
import net.ooici.eoi.netcdf.VariableParams;


/**
 * The AbstractObservationGroup class provides an implementation of common IObservationGroup methods which can be used across
 * common concrete ObservationGroup* classes.
 * 
 * @author cmueller
 */
public abstract class AbstractObservationGroup implements IObservationGroup {

    private static NumberComparator<Number> numComp = new NumberComparator<Number>();

	/** Instance Fields */
	protected String stnid;
	protected int id;
	protected Number lat;
	protected Number lon;
	protected Map<String, String> attributes;
	protected DataType latLonDataType = null;
	protected DataType timeDataType = null;
	protected DataType depthDataType = null;
    protected List<Number> times = new ArrayList<Number>();
	protected List<Number> depths = new ArrayList<Number>();
	

	/**
	 * Constructs a new AbstractObservationGroup with the given identifying characteristics
	 * 
	 * @param id The identifier of this AbstractObservationGroup instance
	 * @param stnid The identifier of the platform/instrument from which this AbstractObservationGroup's observations were made
	 * @param lat The geographic latitude coordinate at which this AbstractObservationGroup's observations were made
	 * @param lon The geographic longitude coordinate at which this AbstractObservationGroup's observations were made
	 */
	public AbstractObservationGroup(int id, String stnid, Number lat, Number lon) {
		this.id = id;
		this.stnid = stnid;
		this.lat = lat;
		this.lon = lon;
		if (!lat.getClass().equals(lon.getClass())) {
		    throw new IllegalArgumentException("lat and lon values are not of the same datatype");
		}
		this.latLonDataType = DataType.getDataType(lat.getClass());
		if (null == latLonDataType) throw new IllegalArgumentException("The given class type '" + lat.getClass() + "' is not supported for lat/lon values; this class has not been implemented in the DataType enum");
		attributes = new HashMap<String, String>();
	}

    

    @Override
    public boolean isEmpty() {
        return times.isEmpty();
    }

    @Override
	public String getStnid() {
		return stnid;
	}

    @Override
	public int getId() {
		return id;
	}

    @Override
	public Number getLat() {
		return lat;
	}

    @Override
	public Number getLon() {
		return lon;
	}

    @Override
	public Number[] getTimes() {
		return getTimes(new Number[times.size()]);
	}
	
    @Override
	public <T> T[] getTimes(T[] array) {
        Collections.sort(times, numComp);
	    return times.toArray(array);
	}

    @Override
	public Number[] getDepths() {
	    return getDepths(new Number[depths.size()]);
	}
	
    @Override
	public <T> T[] getDepths(T[] array) {
        Collections.sort(depths, numComp);
	    return depths.toArray(array);
	}

    @Override
	public final void addObservation(Number time, Number depth, Number data, VariableParams dataAttribs) {
        /** Lazy-initialize the datatypes */
        if (null == timeDataType) {
            timeDataType = DataType.getDataType(time.getClass());
            if (null == timeDataType) throw new IllegalArgumentException("The given class type '" + time.getClass() + "' is not supported for time values; this class has not been implemented in the DataType enum");
        }
        if (null == depthDataType) {
            depthDataType = DataType.getDataType(depth.getClass());
            if (null == depthDataType) throw new IllegalArgumentException("The given class type '" + depth.getClass() + "' is not supported for depth values; this class has not been implemented in the DataType enum");
        }
        
        
        /** Sentinel Checks: data type */
        if (!time.getClass().equals(timeDataType.getDataTypeClass())) {
            throw new IllegalArgumentException("Values for 'time' must be of the same type; type is now: " + timeDataType.getDataTypeClass());
        }
        if (!depth.getClass().equals(depthDataType.getDataTypeClass())) {
            throw new IllegalArgumentException("Values for 'depth' must be of the same type; type is now: " + depthDataType.getDataTypeClass());
        }
        if (!data.getClass().equals(dataAttribs.getDataType().getDataTypeClass())) {
            throw new IllegalArgumentException("Values for 'data' must match the data type of the given dataAttribs; this type is (dataAttribs.getDataType().getDataTypeClass()): " + dataAttribs.getDataType().getDataTypeClass());
        }
        
        _addObservation(time, depth, data, dataAttribs);
	}
	
	protected abstract void _addObservation(Number time, Number depth, Number data, VariableParams dataAttribs);

    @Override
	public String addAttribute(String name, String value) {
		return attributes.put(name, value);
	}
	
    @Override
	public String getAttribute(String name) {
		return attributes.get(name);
	}
	
    @Override
	public void addAttributes(Map<? extends String, ? extends String> values) {
		attributes.putAll(values);
	}

	@Override
	public Map<String, String> getAttributes() {
		return attributes;
	}
	
    @Override
    public DataType getLatLonDataType() {
        return latLonDataType;
    }

    @Override
    public DataType getTimeDataType() {
        return timeDataType;
    }

    @Override
    public DataType getDepthDataType() {
        return depthDataType;
    }

	/**
	 * Inner class used to represent a pairing of time and depth values.  This class is a convienience class
	 * to improve the readibility of the use of ComparablePair instances
	 * 
	 * @author cmueller
	 */
	protected class TimeDepthPair extends Pair<Number, Number> {

	    /**
	     * Constructs a new TimeDepthPair with the given arguments
	     * @param time a temporal value
	     * @param depth a vertical depth value
	     */
		public TimeDepthPair(Number time, Number depth) {
			super(time, depth);
		}

	}
}
