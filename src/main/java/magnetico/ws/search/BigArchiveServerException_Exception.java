
package magnetico.ws.search;

import javax.xml.ws.WebFault;


/**
 * This class was generated by the JAX-WS RI.
 * JAX-WS RI 2.1.5-b03-
 * Generated source version: 2.1
 * 
 */
@WebFault(name = "BigArchiveServerException", targetNamespace = "http://search.bigarchive.magnetosoft.ru/")
public class BigArchiveServerException_Exception
    extends Exception
{

    /**
     * Java type that goes as soapenv:Fault detail element.
     * 
     */
    private BigArchiveServerException faultInfo;

    /**
     * 
     * @param message
     * @param faultInfo
     */
    public BigArchiveServerException_Exception(String message, BigArchiveServerException faultInfo) {
        super(message);
        this.faultInfo = faultInfo;
    }

    /**
     * 
     * @param message
     * @param faultInfo
     * @param cause
     */
    public BigArchiveServerException_Exception(String message, BigArchiveServerException faultInfo, Throwable cause) {
        super(message, cause);
        this.faultInfo = faultInfo;
    }

    /**
     * 
     * @return
     *     returns fault bean: magnetico.ws.search.BigArchiveServerException
     */
    public BigArchiveServerException getFaultInfo() {
        return faultInfo;
    }

}
