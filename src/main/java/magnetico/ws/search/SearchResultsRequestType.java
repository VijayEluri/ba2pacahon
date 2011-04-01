
package magnetico.ws.search;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for searchResultsRequestType complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="searchResultsRequestType">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="contextName" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="expectedQuantity" type="{http://www.w3.org/2001/XMLSchema}int"/>
 *         &lt;element name="fromPosition" type="{http://www.w3.org/2001/XMLSchema}int"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "searchResultsRequestType", propOrder = {
    "contextName",
    "expectedQuantity",
    "fromPosition"
})
public class SearchResultsRequestType {

    protected String contextName;
    protected int expectedQuantity;
    protected int fromPosition;

    /**
     * Gets the value of the contextName property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getContextName() {
        return contextName;
    }

    /**
     * Sets the value of the contextName property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setContextName(String value) {
        this.contextName = value;
    }

    /**
     * Gets the value of the expectedQuantity property.
     * 
     */
    public int getExpectedQuantity() {
        return expectedQuantity;
    }

    /**
     * Sets the value of the expectedQuantity property.
     * 
     */
    public void setExpectedQuantity(int value) {
        this.expectedQuantity = value;
    }

    /**
     * Gets the value of the fromPosition property.
     * 
     */
    public int getFromPosition() {
        return fromPosition;
    }

    /**
     * Sets the value of the fromPosition property.
     * 
     */
    public void setFromPosition(int value) {
        this.fromPosition = value;
    }

}
