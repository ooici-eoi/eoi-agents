/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package net.ooici.eoi.datasetagent;

/**
 *
 * @author cmueller
 */
public class ControlEvent extends java.util.EventObject {

    public enum ControlEventType {

        STATUS,
        SHUTDOWN,
        UPDATE,
        INGEST_ERROR
    }
    
    private ControlEventType type;
    private ion.core.messaging.IonMessage msg;

    public ControlEvent(Object source, ControlEventType type) {
        this(source, type, null);
    }

    public ControlEvent(Object source, ControlEventType type, final ion.core.messaging.IonMessage msg) {
        super(source);
        this.type = type;
        this.msg = msg;
    }

    public ControlEventType getEventType() {
        return type;
    }

    public ion.core.messaging.IonMessage getIonMessage() {
        return msg;
    }
}
