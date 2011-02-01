/*
 * File Name:  AbstractAsciiAgent.java
 * Created on: Dec 17, 2010
 */
package net.ooici.eoi.datasetagent;

import net.ooici.eoi.datasetagent.obs.IObservationGroup;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.TimeZone;

import ucar.nc2.dataset.NetcdfDataset;

/**
 * TODO Add class comments
 * 
 * @author tlarocque
 * @version 1.0
 */
public abstract class AbstractAsciiAgent extends AbstractDatasetAgent implements IAsciiAgent {

    /** Static Fields */
    static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(AbstractAsciiAgent.class);
    protected static final SimpleDateFormat outSdf;
    static {
        outSdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        outSdf.setTimeZone(TimeZone.getTimeZone("UTC"));
    }
    
    
    
    /* (non-Javadoc)
     * @see net.ooici.agent.abstraction.IDatasetAgent#acquireData(java.lang.String)
     */
    @Override
    public Object acquireData(String request) {
        /* ASCII data requests are assumed to be basic HTTP post requests, or references to local files */
        log.debug("");
        log.info("Acquiring data for request [" + request.substring(0, Math.min(40, request.length())) + "...]");

        String data = AgentUtils.getDataString(request);
        log.debug("... acquired raw data: [" + data.substring(0, Math.min(1000, data.length())) + "...]");
        
        return data;
    }
    
    /* (non-Javadoc)
     * @see net.ooici.agent.abstraction.AbstractDatasetAgent#buildDataset(java.lang.Object)
     */
    @Override
    protected final NetcdfDataset buildDataset(Object data) {
        if (!(data instanceof String)) {
            throw new IllegalArgumentException(new StringBuilder("Supplied data must an instance of ")
                                               .append(String.class.getName())
                                               .append("; '")
                                               .append(data.getClass().getName())
                                               .append("' type was received").toString());
        }
        return buildDataset((String) data);
    }
    
    /* (non-Javadoc)
     * @see net.ooici.agent.abstraction.IAsciiAgent#buildDataset(java.lang.String)
     */
    @Override
    public final NetcdfDataset buildDataset(String asciiData) {
        /* NOTE: Template method.  Do not reorder */
        NetcdfDataset ncds = null;
        List<IObservationGroup> obs = null;
        
        obs = parseObs(asciiData);
        ncds = obs2Ncds(obs);
        
        return ncds;
    }
 
    /* TODO: Can we assume all requests from Ascii agents will be for URLs? */
    
    abstract protected List<IObservationGroup> parseObs(String asciiData);
    
    abstract protected NetcdfDataset obs2Ncds(List<IObservationGroup> observations);
}
