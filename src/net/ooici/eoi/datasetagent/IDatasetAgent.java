/*
 * File Name:  IDatasetAgent.java
 * Created on: Dec 17, 2010
 */
package net.ooici.eoi.datasetagent;

import ucar.nc2.dataset.NetcdfDataset;


/**
 * IDatasetAgent provides a generic contract for performing updates on OOI Datasets. The primary entry-point to the dataset update mechanism
 * is {@link #doUpdate(net.ooici.services.sa.DataSource.EoiDataContextMessage, java.util.HashMap)}, which is responsible for updating a
 * service defined by connection parameters in a <code>HashMap</code> using the current state of the dataset defined by an
 * <code>EoiDataContextMessage</code>. The remaining methods {@link #buildRequest(net.ooici.services.sa.DataSource.EoiDataContextMessage)}
 * and {@link #acquireData(String)} can be utilized by <code>doUpdate()</code> to produced the updated dataset before it is pushed to the
 * aforementioned service.
 * 
 * @author cmueller
 * @author tlarocque
 * @version 1.0
 */
public interface IDatasetAgent {

    /**
     * Generates the request that should be used when retrieving data. This request is passed directly to {@link #acquireData(String)}, and
     * so implementations should define these two methods to work in tandem in completing the request/response portion of a dataset update.
     * Typically, <code>buildRequest()</code> returns a URL, local file, or OPeNDAP request for retrieving data.
     * 
     * @param context
     *            the current or required state of a given dataset providing context for building data requests to fulfill dataset updates
     * 
     * @return a data/dataset request which {@link #acquireData(String)} can fulfill
     * @see #acquireData(String)
     * 
     */
    String buildRequest(net.ooici.services.sa.DataSource.EoiDataContextMessage context);

    /**
     * Produces a response to the given <code>request</code>. Implementations may further define the format of the request and resultant
     * data. Typical implementations returns data/datasets in the form of Ascii (CST/TSV) <code>String</code>s or {@link NetcdfDataset}
     * objects.
     * 
     * @param request
     *            A data acquisition request to be fulfilled. Implementations may define the structure of this methods result.
     * 
     * @return the data/dataset result for the given <code>request</code>
     * @see #buildRequest(net.ooici.services.sa.DataSource.EoiDataContextMessage)
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
     * Determines the "runType" for the agent instance. In all cases, updates are produced.  However, only with <code>AgentRunType.NORMAL</code> are
     * any messages sent.  See {@link net.ooici.eoi.datasetagent.AbstractDatasetAgent.AgentRunType} for details about the various options.
     * 
     * @param isTest
     *            Whether or not we are testing
     */
    void setAgentRunType(net.ooici.eoi.datasetagent.AbstractDatasetAgent.AgentRunType agentRunType);

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
