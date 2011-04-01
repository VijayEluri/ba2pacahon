
package magnetico.ws.search;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for searchAsync complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="searchAsync">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="sessionTicketId" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="searchRequest" type="{http://search.bigarchive.magnetosoft.ru/}searchRequestType" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "searchAsync", propOrder = {
    "sessionTicketId",
    "searchRequest"
})
public class SearchAsync {

    protected String sessionTicketId;
    protected SearchRequestType searchRequest;

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
     * Gets the value of the searchRequest property.
     * 
     * @return
     *     possible object is
     *     {@link SearchRequestType }
     *     
     */
    public SearchRequestType getSearchRequest() {
        return searchRequest;
    }

    /**
     * Sets the value of the searchRequest property.
     * 
     * @param value
     *     allowed object is
     *     {@link SearchRequestType }
     *     
     */
    public void setSearchRequest(SearchRequestType value) {
        this.searchRequest = value;
    }

}
