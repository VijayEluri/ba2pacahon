
package magnetico.ws.document;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for updateDocumentTemplateDraft complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="updateDocumentTemplateDraft">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="sessionTicketId" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="draft" type="{http://documents.bigarchive.magnetosoft.ru/}documentTemplateDraftType" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "updateDocumentTemplateDraft", propOrder = {
    "sessionTicketId",
    "draft"
})
public class UpdateDocumentTemplateDraft {

    protected String sessionTicketId;
    protected DocumentTemplateDraftType draft;

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
     * Gets the value of the draft property.
     * 
     * @return
     *     possible object is
     *     {@link DocumentTemplateDraftType }
     *     
     */
    public DocumentTemplateDraftType getDraft() {
        return draft;
    }

    /**
     * Sets the value of the draft property.
     * 
     * @param value
     *     allowed object is
     *     {@link DocumentTemplateDraftType }
     *     
     */
    public void setDraft(DocumentTemplateDraftType value) {
        this.draft = value;
    }

}
