/*
 * File Name:  IAsciiAgent.java
 * Created on: Dec 17, 2010
 */
package net.ooici.eoi.datasetagent;

import net.ooici.eoi.datasetagent.obs.IObservationGroup;

/**
 * TODO Add class comments
 *
 * @author cmueller
 * @author tlarocque
 * @version 1.0
 */
public interface IAsciiAgent extends IDatasetAgent {

    String[] processDataset(IObservationGroup... obsList);
    
}
