/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package net.ooici.eoi.datasetagent;

import net.ooici.eoi.datasetagent.impl.*;


/**
 *
 * @author cmueller
 */
public class AgentFactory {
    public static IDatasetAgent getDatasetAgent(net.ooici.services.sa.DataSource.SourceType st) throws IllegalArgumentException {
        IDatasetAgent agent = null;
        switch (st) {
            case SOS:
                agent = new SosAgent();
                break;
            case USGS:
                agent = new UsgsAgent();
                break;
            case AOML:
                agent = new AomlAgent();
                break;
            case NETCDF_S:
            case NETCDF_C:
                agent = new NcAgent();
                break;
            default:
                throw new IllegalArgumentException("Invalid source_type = " + st.toString());
        }
        return agent;
    }
}
