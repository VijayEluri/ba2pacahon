
package magnetico.ws.document;

import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for jaxbObjectType.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * <p>
 * <pre>
 * &lt;simpleType name="jaxbObjectType">
 *   &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string">
 *     &lt;enumeration value="DICTIONARY"/>
 *     &lt;enumeration value="DOCUMENT"/>
 *     &lt;enumeration value="ORGANIZATION"/>
 *   &lt;/restriction>
 * &lt;/simpleType>
 * </pre>
 * 
 */
@XmlType(name = "jaxbObjectType")
@XmlEnum
public enum JaxbObjectType {

    DICTIONARY,
    DOCUMENT,
    ORGANIZATION;

    public String value() {
        return name();
    }

    public static JaxbObjectType fromValue(String v) {
        return valueOf(v);
    }

}
