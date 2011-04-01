
package magnetico.ws.document;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for getDocumentTemplates complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="getDocumentTemplates">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="sessionTicketId" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="objectType" type="{http://documents.bigarchive.magnetosoft.ru/}jaxbObjectType" minOccurs="0"/>
 *         &lt;element name="withAttributes" type="{http://www.w3.org/2001/XMLSchema}boolean"/>
 *         &lt;element name="forDocumentCreating" type="{http://www.w3.org/2001/XMLSchema}boolean"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "getDocumentTemplates", propOrder = {
    "sessionTicketId",
    "objectType",
    "withAttributes",
    "forDocumentCreating"
})
public class GetDocumentTemplates {

    protected String sessionTicketId;
    protected JaxbObjectType objectType;
    protected boolean withAttributes;
    protected boolean forDocumentCreating;

    /**
     * Gets the value of the sessionTicketId property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getSessionTicketId() {
        return sessionTicketId;
    }

    /**
     * Sets the value of the sessionTicketId property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setSessionTicketId(String value) {
        this.sessionTicketId = value;
    }

    /**
     * Gets the value of the objectType property.
     * 
     * @return
     *     possible object is
     *     {@link JaxbObjectType }
     *     
     */
    public JaxbObjectType getObjectType() {
        return objectType;
    }

    /**
     * Sets the value of the objectType property.
     * 
     * @param value
     *     allowed object is
     *     {@link JaxbObjectType }
     *     
     */
    public void setObjectType(JaxbObjectType value) {
        this.objectType = value;
    }

    /**
     * Gets the value of the withAttributes property.
     * 
     */
    public boolean isWithAttributes() {
        return withAttributes;
    }

    /**
     * Sets the value of the withAttributes property.
     * 
     */
    public void setWithAttributes(boolean value) {
        this.withAttributes = value;
    }

    /**
     * Gets the value of the forDocumentCreating property.
     * 
     */
    public boolean isForDocumentCreating() {
        return forDocumentCreating;
    }

    /**
     * Sets the value of the forDocumentCreating property.
     * 
     */
    public void setForDocumentCreating(boolean value) {
        this.forDocumentCreating = value;
    }

}
