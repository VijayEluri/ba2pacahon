
package com.bigarchive.filemanager;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for anonymous complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType>
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="header" type="{http://filemanager.zdms_component.mndsc.ru/}requestHeaderType"/>
 *         &lt;element name="fileEntry" type="{http://filemanager.zdms_component.mndsc.ru/}fileEntryType"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "", propOrder = {
    "header",
    "fileEntry"
})
@XmlRootElement(name = "storeFileRequestEnvelope")
public class StoreFileRequestEnvelope {

    @XmlElement(required = true)
    protected RequestHeaderType header;
    @XmlElement(required = true)
    protected FileEntryType fileEntry;

    /**
     * Gets the value of the header property.
     * 
     * @return
     *     possible object is
     *     {@link RequestHeaderType }
     *     
     */
    public RequestHeaderType getHeader() {
        return header;
    }

    /**
     * Sets the value of the header property.
     * 
     * @param value
     *     allowed object is
     *     {@link RequestHeaderType }
     *     
     */
    public void setHeader(RequestHeaderType value) {
        this.header = value;
    }

    /**
     * Gets the value of the fileEntry property.
     * 
     * @return
     *     possible object is
     *     {@link FileEntryType }
     *     
     */
    public FileEntryType getFileEntry() {
        return fileEntry;
    }

    /**
     * Sets the value of the fileEntry property.
     * 
     * @param value
     *     allowed object is
     *     {@link FileEntryType }
     *     
     */
    public void setFileEntry(FileEntryType value) {
        this.fileEntry = value;
    }

}
