
package magnetico.ws.document;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for createDocumentTemplate complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="createDocumentTemplate">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="sessionTicketId" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="newDocumentTemplate" type="{http://documents.bigarchive.magnetosoft.ru/}documentTemplateType" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "createDocumentTemplate", propOrder = {
    "sessionTicketId",
    "newDocumentTemplate"
})
public class CreateDocumentTemplate {

    protected String sessionTicketId;
    protected DocumentTemplateType newDocumentTemplate;

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
     * Gets the value of the newDocumentTemplate property.
     * 
     * @return
     *     possible object is
     *     {@link DocumentTemplateType }
     *     
     */
    public DocumentTemplateType getNewDocumentTemplate() {
        return newDocumentTemplate;
    }

    /**
     * Sets the value of the newDocumentTemplate property.
     * 
     * @param value
     *     allowed object is
     *     {@link DocumentTemplateType }
     *     
     */
    public void setNewDocumentTemplate(DocumentTemplateType value) {
        this.newDocumentTemplate = value;
    }

}
