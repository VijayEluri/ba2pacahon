
package com.bigarchive.filemanager;

import javax.xml.ws.WebFault;


/**
 * This class was generated by the JAX-WS RI.
 * JAX-WS RI 2.1.6 in JDK 6
 * Generated source version: 2.1
 * 
 */
@WebFault(name = "storeFileFaultEnvelope", targetNamespace = "http://filemanager.zdms_component.mndsc.ru/")
public class StoreFileFault
    extends Exception
{

    /**
     * Java type that goes as soapenv:Fault detail element.
     * 
     */
    private StoreFileFaultEnvelope faultInfo;

    /**
     * 
     * @param message
     * @param faultInfo
     */
    public StoreFileFault(String message, StoreFileFaultEnvelope faultInfo) {
        super(message);
        this.faultInfo = faultInfo;
    }

    /**
     * 
     * @param message
     * @param faultInfo
     * @param cause
     */
    public StoreFileFault(String message, StoreFileFaultEnvelope faultInfo, Throwable cause) {
        super(message, cause);
        this.faultInfo = faultInfo;
    }

    /**
     * 
     * @return
     *     returns fault bean: com.bigarchive.filemanager.StoreFileFaultEnvelope
     */
    public StoreFileFaultEnvelope getFaultInfo() {
        return faultInfo;
    }

}
