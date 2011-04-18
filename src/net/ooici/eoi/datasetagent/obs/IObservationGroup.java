package net.ooici.eoi.datasetagent.obs;


import java.util.List;
import java.util.Map;
import net.ooici.eoi.netcdf.VariableParams;


/**
 * The IObservationGroup specifies the interface for implementing containers to store groupings of one or more data observations at a fixed
 * geographic point.
 * 
 * @author tlarocque
 */
public interface IObservationGroup {

    public enum DataType {
        INT(Integer.class),
        LONG(Long.class),
        FLOAT(Float.class),
        DOUBLE(Double.class);
        
        Class<?> cls = null;
        DataType(Class<?> cls) {
            this.cls = cls;
        }
        
        public Class<?> getDataTypeClass(){
            return cls;
        }
        
        public static DataType getDataType(Class<?> cls) {
            DataType result = null;
            for (DataType dt : DataType.values()) {
                if (cls.equals(dt.getDataTypeClass())) {
                    result = dt;
                    break;
                }
            }
            return result;
        }
        
        public static DataType typeOf(Object o) {
            return getDataType(o.getClass());
        }
    }

	/* Platform-related Methods */
    /**
     * Retrieves the identifier of this IObservationGroup Object
     */
	int getId();

	/**
	 * @return the identifier of the unit from which this IObservationGroup's observations were made
	 */
	String getStnid();

	/**
	 * @return the geographic latitude coordinate at which this IObservationGroup's observations were made
	 */
	Number getLat();

	/**
	 * @return the geographic longitude coordinate at which this IObservationGroup's observations were made
	 */
	Number getLon();


        DataType getLatLonDataType();


	/* Observation Methods */

	/**
	 * Adds the given data as an observation to this IObservationGroup's list of observations
	 */
	void addObservation(Number time, Number depth, Number data, VariableParams dataAttribs);

	/**
	 * @return the total number of observations in this IObservationGroup's list of observations
	 */
	int getNumObs();

    /**
     * @return true if this group has zero observations; false otherwise
     */
    boolean isEmpty();

	/**
	 * @return an array of all unique timestamps for the observations in this IObservationGroup
	 */
	Number[] getTimes();

        /**
         * Returns an array containing all of the unique timestamp entries for the observations in this IObservationGroup; the runtime type of the
         * returned array is that of the specified <code>array</code>. If these entries fit in the specified array, they are returned therein. Otherwise, a new
         * array is allocated with the runtime type of the specified array and the size of the number of unique timestamp values currently stored.<br />
         * <br />
         * If the list fits in the specified array with room to spare (i.e., the array has more elements than the list), the element in the
         * array immediately following the end of the list is set to null. (This is useful in determining the length of the list only if the
         * caller knows that the list does not contain any null elements.)
         * 
         * @param <T> The type of array which will be returned
         * @param array
         *          the array into which the elements of this list are to be stored, if it is big enough; otherwise, a new array of the same runtime type is allocated for this purpose.
         * 
         * @return an array of all unique timestamps for the observations in this IObservationGroup with a type equal to the datatype of the
         *         given <code>array</code>
         *         
         * @throws ArrayStoreException
         *          if the runtime type of the specified array is not a supertype of the runtime type of every element in this list
         * @throws NullPointerException
         *          if the given <code>array</code> is <code>null</code>
         */
	    <T> T[] getTimes(T[] array);
	    /**
	     * @return A <code>DataType</code> object representing the type of storage for this IObservationGroup's time data
	     */
        DataType getTimeDataType();

	/**
	 * @return an array of all unique depth values for the observations in this IObservationGroup
	 */
	Number[] getDepths();

    	/**
         * Returns an array containing all of the unique depth entries for the observations in this IObservationGroup; the runtime type of the
         * returned array is that of the specified <code>array</code>. If the list fits in the specified array, it is returned therein. Otherwise, a new
         * array is allocated with the runtime type of the specified array and the size of the number of unique depth values currently stored.<br />
         * <br />
         * If the list fits in the specified array with room to spare (i.e., the array has more elements than the list), the element in the
         * array immediately following the end of the list is set to null. (This is useful in determining the length of the list only if the
         * caller knows that the list does not contain any null elements.)
         * 
         * @param <T> The type of array which will be returned
         * @param array
         *          the array into which the depth values are to be stored, if it is big enough; otherwise, a new array of the same runtime type is allocated for this purpose.
         * 
         * @return an array of all unique depth values for the observations in this IObservationGroup with a type equal to the datatype of the
         *         given <code>array</code>
         *         
         * @throws ArrayStoreException
         *          if the runtime type of the specified array is not a supertype of the runtime type of every element in this list
         * @throws NullPointerException
         *          if the given <code>array</code> is <code>null</code>
         */
        <T> T[] getDepths(T[] array);
        /**
         * @return A <code>DataType</code> object representing the type of storage for this IObservationGroup's depth data
         */
        DataType getDepthDataType();

	/**
	 * Retrieves the data value for the variable hinted by the given VariableParams at the given time and depth
	 * 
	 * @param dataAttribs an instance of VariableParams to represent the type of data which should be retrieved (ie. the variable)
	 * @param time a temporal value which corresponds to the same storage conventions as the time value in addObservation()
	 * @param depth a vertical depth value which corresponds to the same storage conventions as the depth value in addObservation()
	 * 
	 * @return If a data value exists for the parameters given, that data is returned; otherwise <code>null</code>
	 */
	Number getData(VariableParams dataAttribs, Number time, Number depth);

	/**
	 * Retrieves the data value for the variable hinted by the given VariableParams at the given time and depth or returns <code>missingVal</code>
	 * when no data is present
	 * 
	 * @param dataAttribs an instance of VariableParams to represent the type of data which should be retrieved (ie. the variable)
	 * @param time a temporal value which corresponds to the same storage conventions as the time value in addObservation()
	 * @param depth a vertical depth value which corresponds to the same storage conventions as the depth value in addObservation()
	 * @param missingVal the data value which should be returned if no data can be acquired for the previous arguments
	 * 
	 * @return If a data value exists for the parameters given, that data is returned; otherwise missingVal is returned
	 */
	Number getData(VariableParams dataAttribs, Number time, Number depth, Number missingVal);

	/**
	 * @return a list of all unique VariableAttribute values for the observations in thsi IObservationGroup.  This list
	 * is essentially a group of all the variables of which data may exist, in this IObservationGroup object
	 */
	List<VariableParams> getDataNames();
	
	
	
	/* Attribute Methods */
	
	String addAttribute(String name, String value);
	
	String getAttribute(String name);
	
	void addAttributes(Map<? extends String, ? extends String> values);
	
	Map<String, String> getAttributes();
	
}
