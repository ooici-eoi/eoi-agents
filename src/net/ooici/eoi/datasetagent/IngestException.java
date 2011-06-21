/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package net.ooici.eoi.datasetagent;

import ion.core.IonException;

/**
 *
 * @author cmueller
 */
public class IngestException extends IonException {

    public IngestException() {
        super();
    }
    public IngestException(String message) {
        super(message);
    }
    public IngestException(Throwable cause){
        super(cause);
    }
    public IngestException(String message, Throwable cause) {
        super(message, cause);
    }
    
}
