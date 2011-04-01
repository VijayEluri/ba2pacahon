
package magnetico.ws.document;

import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for confirmStates.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * <p>
 * <pre>
 * &lt;simpleType name="confirmStates">
 *   &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string">
 *     &lt;enumeration value="NONE"/>
 *     &lt;enumeration value="OK"/>
 *     &lt;enumeration value="FAIL"/>
 *     &lt;enumeration value="SKIPPED"/>
 *   &lt;/restriction>
 * &lt;/simpleType>
 * </pre>
 * 
 */
@XmlType(name = "confirmStates")
@XmlEnum
public enum ConfirmStates {

    NONE,
    OK,
    FAIL,
    SKIPPED;

    public String value() {
        return name();
    }

    public static ConfirmStates fromValue(String v) {
        return valueOf(v);
    }

}
