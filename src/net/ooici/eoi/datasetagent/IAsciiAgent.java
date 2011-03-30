/*
 * File Name:  IAsciiAgent.java
 * Created on: Dec 17, 2010
 */
package net.ooici.eoi.datasetagent;


import net.ooici.eoi.datasetagent.obs.IObservationGroup;


/**
 * The <code>IAsciiAgent</code> interface extends the <code>IDatasetAgent</code> interface by providing a publically accessible method for
 * processing a list of <code>IObservationGroup</code> objects interpretted as datasets. Typical implementations of this interface define
 * dataset processing as the actions required in interpreting a list of <code>IObservationGroup</code>'s as a dataset object, breaking that
 * dataset into manageable sections and passing those object(s) to a service for ingestion/persistence and the like
 * 
 * 
 * @author cmueller
 * @author tlarocque
 * @version 1.0
 */
public interface IAsciiAgent extends IDatasetAgent {

    /**
     * Typical implementations define processDataset() by interpreting the given list of <code>IObservationGroup</code>'s as a dataset
     * object, breaking that dataset into manageable sections and passing those object(s) to a service for ingestion/persistence and the
     * like
     * 
     * @param obsList
     *            a list of <code>IObservationGroup</code> objects
     * 
     * @return TODO:
     */
    String[] processDataset(IObservationGroup... obsList);

}
