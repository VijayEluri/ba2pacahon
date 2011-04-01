
package magnetico.ws.document;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for updateDocumentTemplate complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="updateDocumentTemplate">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="sessionTicketId" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="documentTemplate" type="{http://documents.bigarchive.magnetosoft.ru/}documentTemplateType" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "updateDocumentTemplate", propOrder = {
    "sessionTicketId",
    "documentTemplate"
})
public class UpdateDocumentTemplate {

    protected String sessionTicketId;
    protected DocumentTemplateType documentTemplate;

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
     * Gets the value of the documentTemplate property.
     * 
     * @return
     *     possible object is
     *     {@link DocumentTemplateType }
     *     
     */
    public DocumentTemplateType getDocumentTemplate() {
        return documentTemplate;
    }

    /**
     * Sets the value of the documentTemplate property.
     * 
     * @param value
     *     allowed object is
     *     {@link DocumentTemplateType }
     *     
     */
    public void setDocumentTemplate(DocumentTemplateType value) {
        this.documentTemplate = value;
    }

}
