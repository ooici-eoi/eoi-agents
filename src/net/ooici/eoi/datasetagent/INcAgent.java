/*
 * File Name:  INcAgent.java
 * Created on: Dec 17, 2010
 */
package net.ooici.eoi.datasetagent;


/**
 * The <code>INcAgent</code> interface extends the <code>IDatasetAgent</code> interface by providing a publically accessible method for
 * processing a <code>NetcdfDataset</code> object.<br />
 * <br />
 * Typical implementations of this interface define dataset processing as the actions
 * required in breaking the given dataset into manageable sections and passing those object(s) to a service for ingestion/persistence and
 * the like
 * 
 * @author cmueller
 * @author tlarocque
 * @version 1.0
 */
public interface INcAgent extends IDatasetAgent {

    /**
     * Typical implementations define processDataset() by breaking the given dataset into manageable sections and passing those object(s) to
     * a service for ingestion/persistence and the like
     * 
     * @param obsList
     *            a list of <code>IObservationGroup</code> objects
     * 
     * @return TODO:
     */
    String[] processDataset(ucar.nc2.dataset.NetcdfDataset ncds);

}
