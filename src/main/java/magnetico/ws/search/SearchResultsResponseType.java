
package magnetico.ws.search;

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for searchResultsResponseType complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="searchResultsResponseType">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="inProcess" type="{http://www.w3.org/2001/XMLSchema}boolean"/>
 *         &lt;element name="results" type="{http://search.bigarchive.magnetosoft.ru/}searchResultResponseType" maxOccurs="unbounded" minOccurs="0"/>
 *         &lt;element name="totalCount" type="{http://www.w3.org/2001/XMLSchema}long"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "searchResultsResponseType", propOrder = {
    "inProcess",
    "results",
    "totalCount"
})
public class SearchResultsResponseType {

    protected boolean inProcess;
    protected List<SearchResultResponseType> results;
    protected long totalCount;

    /**
     * Gets the value of the inProcess property.
     * 
     */
    public boolean isInProcess() {
        return inProcess;
    }

    /**
     * Sets the value of the inProcess property.
     * 
     */
    public void setInProcess(boolean value) {
        this.inProcess = value;
    }

    /**
     * Gets the value of the results property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the results property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getResults().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link SearchResultResponseType }
     * 
     * 
     */
    public List<SearchResultResponseType> getResults() {
        if (results == null) {
            results = new ArrayList<SearchResultResponseType>();
        }
        return this.results;
    }

    /**
     * Gets the value of the totalCount property.
     * 
     */
    public long getTotalCount() {
        return totalCount;
    }

    /**
     * Sets the value of the totalCount property.
     * 
     */
    public void setTotalCount(long value) {
        this.totalCount = value;
    }

}
