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

    public enum SourceType {

        SOS,
        AOML,
        NC_GRID,
        USGS,
        RADS
    }

    public static IDatasetAgent getDatasetAgent(String source_type) throws IllegalArgumentException {
        SourceType st;
        try {
            st = SourceType.valueOf(source_type.toUpperCase());
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException("Invalid source_type = " + source_type.toUpperCase());
        }
        return getDatasetAgent(st);
    }

    public static IDatasetAgent getDatasetAgent(SourceType st) throws IllegalArgumentException {
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
            case RADS:
            case NC_GRID:
            default:
                throw new IllegalArgumentException("Invalid source_type = " + st.toString());
        }
        return agent;
    }
}
