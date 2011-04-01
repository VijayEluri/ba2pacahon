
package magnetico.ws.search;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for getSearchResults complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="getSearchResults">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="sessionTicketId" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="searchResultsRequest" type="{http://search.bigarchive.magnetosoft.ru/}searchResultsRequestType" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "getSearchResults", propOrder = {
    "sessionTicketId",
    "searchResultsRequest"
})
public class GetSearchResults {

    protected String sessionTicketId;
    protected SearchResultsRequestType searchResultsRequest;

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
     * Gets the value of the searchResultsRequest property.
     * 
     * @return
     *     possible object is
     *     {@link SearchResultsRequestType }
     *     
     */
    public SearchResultsRequestType getSearchResultsRequest() {
        return searchResultsRequest;
    }

    /**
     * Sets the value of the searchResultsRequest property.
     * 
     * @param value
     *     allowed object is
     *     {@link SearchResultsRequestType }
     *     
     */
    public void setSearchResultsRequest(SearchResultsRequestType value) {
        this.searchResultsRequest = value;
    }

}
