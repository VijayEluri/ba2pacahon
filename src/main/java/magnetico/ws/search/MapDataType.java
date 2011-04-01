
package magnetico.ws.search;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for mapDataType complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="mapDataType">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="map" type="{http://search.bigarchive.magnetosoft.ru/}hashMapType" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "mapDataType", propOrder = {
    "map"
})
public class MapDataType {

    protected HashMapType map;

    /**
     * Gets the value of the map property.
     * 
     * @return
     *     possible object is
     *     {@link HashMapType }
     *     
     */
    public HashMapType getMap() {
        return map;
    }

    /**
     * Sets the value of the map property.
     * 
     * @param value
     *     allowed object is
     *     {@link HashMapType }
     *     
     */
    public void setMap(HashMapType value) {
        this.map = value;
    }

}
