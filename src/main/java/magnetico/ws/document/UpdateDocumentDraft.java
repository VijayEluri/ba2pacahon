
package magnetico.ws.document;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for updateDocumentDraft complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="updateDocumentDraft">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="sessionTicketId" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="draft" type="{http://documents.bigarchive.magnetosoft.ru/}documentDraftType" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "updateDocumentDraft", propOrder = {
    "sessionTicketId",
    "draft"
})
public class UpdateDocumentDraft {

    protected String sessionTicketId;
    protected DocumentDraftType draft;

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
     *     {@link DocumentDraftType }
     *     
     */
    public DocumentDraftType getDraft() {
        return draft;
    }

    /**
     * Sets the value of the draft property.
     * 
     * @param value
     *     allowed object is
     *     {@link DocumentDraftType }
     *     
     */
    public void setDraft(DocumentDraftType value) {
        this.draft = value;
    }

}
