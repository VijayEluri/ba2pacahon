
package com.bigarchive.filemanager;

import java.util.ArrayList;
import java.util.List;
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
 *         &lt;element name="header" type="{http://filemanager.zdms_component.mndsc.ru/}responseHeaderType"/>
 *         &lt;element name="fileEntries">
 *           &lt;complexType>
 *             &lt;complexContent>
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                 &lt;sequence>
 *                   &lt;element name="fileEntry" type="{http://filemanager.zdms_component.mndsc.ru/}fileEntryType" maxOccurs="unbounded"/>
 *                 &lt;/sequence>
 *               &lt;/restriction>
 *             &lt;/complexContent>
 *           &lt;/complexType>
 *         &lt;/element>
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
    "fileEntries"
})
@XmlRootElement(name = "getFilesResponseEnvelope")
public class GetFilesResponseEnvelope {

    @XmlElement(required = true)
    protected ResponseHeaderType header;
    @XmlElement(required = true)
    protected GetFilesResponseEnvelope.FileEntries fileEntries;

    /**
     * Gets the value of the header property.
     * 
     * @return
     *     possible object is
     *     {@link ResponseHeaderType }
     *     
     */
    public ResponseHeaderType getHeader() {
        return header;
    }

    /**
     * Sets the value of the header property.
     * 
     * @param value
     *     allowed object is
     *     {@link ResponseHeaderType }
     *     
     */
    public void setHeader(ResponseHeaderType value) {
        this.header = value;
    }

    /**
     * Gets the value of the fileEntries property.
     * 
     * @return
     *     possible object is
     *     {@link GetFilesResponseEnvelope.FileEntries }
     *     
     */
    public GetFilesResponseEnvelope.FileEntries getFileEntries() {
        return fileEntries;
    }

    /**
     * Sets the value of the fileEntries property.
     * 
     * @param value
     *     allowed object is
     *     {@link GetFilesResponseEnvelope.FileEntries }
     *     
     */
    public void setFileEntries(GetFilesResponseEnvelope.FileEntries value) {
        this.fileEntries = value;
    }


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
     *         &lt;element name="fileEntry" type="{http://filemanager.zdms_component.mndsc.ru/}fileEntryType" maxOccurs="unbounded"/>
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
        "fileEntry"
    })
    public static class FileEntries {

        @XmlElement(required = true)
        protected List<FileEntryType> fileEntry;

        /**
         * Gets the value of the fileEntry property.
         * 
         * <p>
         * This accessor method returns a reference to the live list,
         * not a snapshot. Therefore any modification you make to the
         * returned list will be present inside the JAXB object.
         * This is why there is not a <CODE>set</CODE> method for the fileEntry property.
         * 
         * <p>
         * For example, to add a new item, do as follows:
         * <pre>
         *    getFileEntry().add(newItem);
         * </pre>
         * 
         * 
         * <p>
         * Objects of the following type(s) are allowed in the list
         * {@link FileEntryType }
         * 
         * 
         */
        public List<FileEntryType> getFileEntry() {
            if (fileEntry == null) {
                fileEntry = new ArrayList<FileEntryType>();
            }
            return this.fileEntry;
        }

    }

}
