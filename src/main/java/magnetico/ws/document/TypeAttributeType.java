
package magnetico.ws.document;

import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for typeAttributeType.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * <p>
 * <pre>
 * &lt;simpleType name="typeAttributeType">
 *   &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string">
 *     &lt;enumeration value="DATE"/>
 *     &lt;enumeration value="DICTIONARY"/>
 *     &lt;enumeration value="NUMBER"/>
 *     &lt;enumeration value="TEXT"/>
 *     &lt;enumeration value="FILE"/>
 *     &lt;enumeration value="LINK"/>
 *     &lt;enumeration value="DATEINTERVAL"/>
 *     &lt;enumeration value="ORGANIZATION"/>
 *     &lt;enumeration value="BOOLEAN"/>
 *   &lt;/restriction>
 * &lt;/simpleType>
 * </pre>
 * 
 */
@XmlType(name = "typeAttributeType")
@XmlEnum
public enum TypeAttributeType {

    DATE,
    DICTIONARY,
    NUMBER,
    TEXT,
    FILE,
    LINK,
    DATEINTERVAL,
    ORGANIZATION,
    BOOLEAN;

    public String value() {
        return name();
    }

    public static TypeAttributeType fromValue(String v) {
        return valueOf(v);
    }

}
