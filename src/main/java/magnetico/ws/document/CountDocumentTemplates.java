
package magnetico.ws.document;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for countDocumentTemplates complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="countDocumentTemplates">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="objectType" type="{http://documents.bigarchive.magnetosoft.ru/}jaxbObjectType" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "countDocumentTemplates", propOrder = {
    "objectType"
})
public class CountDocumentTemplates {

    protected JaxbObjectType objectType;

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

}
