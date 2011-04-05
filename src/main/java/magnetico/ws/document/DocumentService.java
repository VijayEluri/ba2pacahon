
package magnetico.ws.document;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.logging.Logger;
import javax.xml.namespace.QName;
import javax.xml.ws.Service;
import javax.xml.ws.WebEndpoint;
import javax.xml.ws.WebServiceClient;
import javax.xml.ws.WebServiceFeature;


/**
 * This class was generated by the JAX-WS RI.
 * JAX-WS RI 2.1.5-b03-
 * Generated source version: 2.1
 * 
 */
@WebServiceClient(name = "DocumentService", targetNamespace = "http://documents.bigarchive.magnetosoft.ru/", wsdlLocation = "file:/home/itiu/work/ba2pacahon/src/main/wsdl/DocumentService.wsdl")
public class DocumentService
    extends Service
{

    private final static URL DOCUMENTSERVICE_WSDL_LOCATION;
    private final static Logger logger = Logger.getLogger(magnetico.ws.document.DocumentService.class.getName());

    static {
        URL url = null;
        try {
            URL baseUrl;
            baseUrl = magnetico.ws.document.DocumentService.class.getResource(".");
            url = new URL(baseUrl, "file:/home/itiu/work/ba2pacahon/src/main/wsdl/DocumentService.wsdl");
        } catch (MalformedURLException e) {
            logger.warning("Failed to create URL for the wsdl Location: 'file:/home/itiu/work/ba2pacahon/src/main/wsdl/DocumentService.wsdl', retrying as a local file");
            logger.warning(e.getMessage());
        }
        DOCUMENTSERVICE_WSDL_LOCATION = url;
    }

    public DocumentService(URL wsdlLocation, QName serviceName) {
        super(wsdlLocation, serviceName);
    }

    public DocumentService() {
        super(DOCUMENTSERVICE_WSDL_LOCATION, new QName("http://documents.bigarchive.magnetosoft.ru/", "DocumentService"));
    }

    /**
     * 
     * @return
     *     returns DocumentEndpoint
     */
    @WebEndpoint(name = "DocumentServiceEndpointPort")
    public DocumentEndpoint getDocumentServiceEndpointPort() {
        return super.getPort(new QName("http://documents.bigarchive.magnetosoft.ru/", "DocumentServiceEndpointPort"), DocumentEndpoint.class);
    }

    /**
     * 
     * @param features
     *     A list of {@link javax.xml.ws.WebServiceFeature} to configure on the proxy.  Supported features not in the <code>features</code> parameter will have their default values.
     * @return
     *     returns DocumentEndpoint
     */
    @WebEndpoint(name = "DocumentServiceEndpointPort")
    public DocumentEndpoint getDocumentServiceEndpointPort(WebServiceFeature... features) {
        return super.getPort(new QName("http://documents.bigarchive.magnetosoft.ru/", "DocumentServiceEndpointPort"), DocumentEndpoint.class, features);
    }

}