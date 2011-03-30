/*
 * File Name:  IDatasetAgent.java
 * Created on: Dec 17, 2010
 */
package net.ooici.eoi.datasetagent;


/**
 * TODO Add class comments
 * 
 * @author cmueller
 * @author tlarocque
 * @version 1.0
 */
public interface IDatasetAgent {

    /**
     * Generates the request that should be used when retrieving data. This request is passed directly to {@link #acquireData(String)}, and
     * so implementations should define these two methods to work in tandem in completing the request/response portion of a dataset update.
     * 
     * @param context
     *            the current or required state of a given dataset providing context for building data requests to fulfill dataset updates
     * 
     * @return a <code>String</code> request which {@link #acquireData(String)} can fulfill
     * 
     */
    String buildRequest(net.ooici.services.sa.DataSource.EoiDataContextMessage context);

    /**
     * Produces a response to the given <code>request</code>. Implementations may further define the format of the request and resultant
     * data.
     * 
     * @param request
     *            A data acquisition request to be fulfilled. Implementations may define the structure of this methods result.
     * 
     * @return the data result for the given <code>request</code>
     */
    Object acquireData(String request);

    /**
     * Performs a dataset update sequence per the requirements in the given <code>context</code>. <code>connectionInfo</code> provides the
     * necessary arguments to establish connectivity and pass the given update to any required endpoints.
     * 
     * @param context
     *            the current or required state of a given dataset providing context for performing updates upon it
     * @param connectionInfo
     *            parameters used in establishing connectivity for sending the results of a dataset update
     * 
     * @return TODO:
     * 
     */
    String[] doUpdate(net.ooici.services.sa.DataSource.EoiDataContextMessage context, java.util.HashMap<String, String> connectionInfo);

    /**
     * Flags this Dataset Agent that updates are being done strictly for testing. While testing, updates are produced but NOT sent to the
     * ingestion service
     * 
     * @param isTest
     *            Whether or not we are testing
     */
    void setTesting(boolean isTest);

    /**
     * Sets the maximum <i>total bytes</i> of <b>data</b> that can be sent in one message.<br />
     * <br />
     * This is the value used to decompose the dataset when sending and does not include wrapper and message size.
     * 
     * @param maxSize
     *            a size in <code>bytes</code>
     */
    void setMaxSize(long maxSize);

    /**
     * This is the value used to divide the dimension length when decomposing within a given dimension.
     * 
     * @param decompDivisor
     *            an <code>int</code> value
     * 
     * @see AbstractDatasetAgent#decompSendVariable(ucar.nc2.Variable, ucar.ma2.Section, int)
     */
    void setDecompDivisor(int decompDivisor);
}
