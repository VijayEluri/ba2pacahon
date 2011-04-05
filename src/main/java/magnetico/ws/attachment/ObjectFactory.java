
package magnetico.ws.attachment;

import javax.xml.bind.JAXBElement;
import javax.xml.bind.annotation.XmlElementDecl;
import javax.xml.bind.annotation.XmlRegistry;
import javax.xml.namespace.QName;


/**
 * This object contains factory methods for each 
 * Java content interface and Java element interface 
 * generated in the magnetico.ws.attachment package. 
 * <p>An ObjectFactory allows you to programatically 
 * construct new instances of the Java representation 
 * for XML content. The Java representation of XML 
 * content can consist of schema derived interfaces 
 * and classes representing the binding of schema 
 * type definitions, element declarations and model 
 * groups.  Factory methods for each of these are 
 * provided in this class.
 * 
 */
@XmlRegistry
public class ObjectFactory {

    private final static QName _ListAttachments_QNAME = new QName("http://attachments.bigarchive.magnetosoft.ru/", "listAttachments");
    private final static QName _GetVersionResponse_QNAME = new QName("http://attachments.bigarchive.magnetosoft.ru/", "getVersionResponse");
    private final static QName _CreateAttachmentResponse_QNAME = new QName("http://attachments.bigarchive.magnetosoft.ru/", "createAttachmentResponse");
    private final static QName _ListAttachmentsResponse_QNAME = new QName("http://attachments.bigarchive.magnetosoft.ru/", "listAttachmentsResponse");
    private final static QName _Attachment_QNAME = new QName("http://attachments.bigarchive.magnetosoft.ru/", "attachment");
    private final static QName _GetAttachmentResponse_QNAME = new QName("http://attachments.bigarchive.magnetosoft.ru/", "getAttachmentResponse");
    private final static QName _GetDependenciesVersions_QNAME = new QName("http://attachments.bigarchive.magnetosoft.ru/", "getDependenciesVersions");
    private final static QName _GetAttachment_QNAME = new QName("http://attachments.bigarchive.magnetosoft.ru/", "getAttachment");
    private final static QName _GetDependenciesVersionsResponse_QNAME = new QName("http://attachments.bigarchive.magnetosoft.ru/", "getDependenciesVersionsResponse");
    private final static QName _GetVersion_QNAME = new QName("http://attachments.bigarchive.magnetosoft.ru/", "getVersion");
    private final static QName _AccessDeniedException_QNAME = new QName("http://attachments.bigarchive.magnetosoft.ru/", "AccessDeniedException");
    private final static QName _CreateAttachment_QNAME = new QName("http://attachments.bigarchive.magnetosoft.ru/", "createAttachment");
    private final static QName _BigArchiveServerException_QNAME = new QName("http://attachments.bigarchive.magnetosoft.ru/", "BigArchiveServerException");

    /**
     * Create a new ObjectFactory that can be used to create new instances of schema derived classes for package: magnetico.ws.attachment
     * 
     */
    public ObjectFactory() {
    }

    /**
     * Create an instance of {@link ListAttachmentsResponse }
     * 
     */
    public ListAttachmentsResponse createListAttachmentsResponse() {
        return new ListAttachmentsResponse();
    }

    /**
     * Create an instance of {@link ListAttachments }
     * 
     */
    public ListAttachments createListAttachments() {
        return new ListAttachments();
    }

    /**
     * Create an instance of {@link AttachmentType }
     * 
     */
    public AttachmentType createAttachmentType() {
        return new AttachmentType();
    }

    /**
     * Create an instance of {@link GetVersionResponse }
     * 
     */
    public GetVersionResponse createGetVersionResponse() {
        return new GetVersionResponse();
    }

    /**
     * Create an instance of {@link BigArchiveServerException }
     * 
     */
    public BigArchiveServerException createBigArchiveServerException() {
        return new BigArchiveServerException();
    }

    /**
     * Create an instance of {@link CreateAttachment }
     * 
     */
    public CreateAttachment createCreateAttachment() {
        return new CreateAttachment();
    }

    /**
     * Create an instance of {@link GetAttachment }
     * 
     */
    public GetAttachment createGetAttachment() {
        return new GetAttachment();
    }

    /**
     * Create an instance of {@link GetDependenciesVersionsResponse }
     * 
     */
    public GetDependenciesVersionsResponse createGetDependenciesVersionsResponse() {
        return new GetDependenciesVersionsResponse();
    }

    /**
     * Create an instance of {@link AccessDeniedException }
     * 
     */
    public AccessDeniedException createAccessDeniedException() {
        return new AccessDeniedException();
    }

    /**
     * Create an instance of {@link GetVersion }
     * 
     */
    public GetVersion createGetVersion() {
        return new GetVersion();
    }

    /**
     * Create an instance of {@link CreateAttachmentResponse }
     * 
     */
    public CreateAttachmentResponse createCreateAttachmentResponse() {
        return new CreateAttachmentResponse();
    }

    /**
     * Create an instance of {@link GetAttachmentResponse }
     * 
     */
    public GetAttachmentResponse createGetAttachmentResponse() {
        return new GetAttachmentResponse();
    }

    /**
     * Create an instance of {@link GetDependenciesVersions }
     * 
     */
    public GetDependenciesVersions createGetDependenciesVersions() {
        return new GetDependenciesVersions();
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link ListAttachments }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://attachments.bigarchive.magnetosoft.ru/", name = "listAttachments")
    public JAXBElement<ListAttachments> createListAttachments(ListAttachments value) {
        return new JAXBElement<ListAttachments>(_ListAttachments_QNAME, ListAttachments.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link GetVersionResponse }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://attachments.bigarchive.magnetosoft.ru/", name = "getVersionResponse")
    public JAXBElement<GetVersionResponse> createGetVersionResponse(GetVersionResponse value) {
        return new JAXBElement<GetVersionResponse>(_GetVersionResponse_QNAME, GetVersionResponse.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link CreateAttachmentResponse }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://attachments.bigarchive.magnetosoft.ru/", name = "createAttachmentResponse")
    public JAXBElement<CreateAttachmentResponse> createCreateAttachmentResponse(CreateAttachmentResponse value) {
        return new JAXBElement<CreateAttachmentResponse>(_CreateAttachmentResponse_QNAME, CreateAttachmentResponse.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link ListAttachmentsResponse }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://attachments.bigarchive.magnetosoft.ru/", name = "listAttachmentsResponse")
    public JAXBElement<ListAttachmentsResponse> createListAttachmentsResponse(ListAttachmentsResponse value) {
        return new JAXBElement<ListAttachmentsResponse>(_ListAttachmentsResponse_QNAME, ListAttachmentsResponse.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link AttachmentType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://attachments.bigarchive.magnetosoft.ru/", name = "attachment")
    public JAXBElement<AttachmentType> createAttachment(AttachmentType value) {
        return new JAXBElement<AttachmentType>(_Attachment_QNAME, AttachmentType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link GetAttachmentResponse }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://attachments.bigarchive.magnetosoft.ru/", name = "getAttachmentResponse")
    public JAXBElement<GetAttachmentResponse> createGetAttachmentResponse(GetAttachmentResponse value) {
        return new JAXBElement<GetAttachmentResponse>(_GetAttachmentResponse_QNAME, GetAttachmentResponse.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link GetDependenciesVersions }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://attachments.bigarchive.magnetosoft.ru/", name = "getDependenciesVersions")
    public JAXBElement<GetDependenciesVersions> createGetDependenciesVersions(GetDependenciesVersions value) {
        return new JAXBElement<GetDependenciesVersions>(_GetDependenciesVersions_QNAME, GetDependenciesVersions.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link GetAttachment }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://attachments.bigarchive.magnetosoft.ru/", name = "getAttachment")
    public JAXBElement<GetAttachment> createGetAttachment(GetAttachment value) {
        return new JAXBElement<GetAttachment>(_GetAttachment_QNAME, GetAttachment.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link GetDependenciesVersionsResponse }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://attachments.bigarchive.magnetosoft.ru/", name = "getDependenciesVersionsResponse")
    public JAXBElement<GetDependenciesVersionsResponse> createGetDependenciesVersionsResponse(GetDependenciesVersionsResponse value) {
        return new JAXBElement<GetDependenciesVersionsResponse>(_GetDependenciesVersionsResponse_QNAME, GetDependenciesVersionsResponse.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link GetVersion }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://attachments.bigarchive.magnetosoft.ru/", name = "getVersion")
    public JAXBElement<GetVersion> createGetVersion(GetVersion value) {
        return new JAXBElement<GetVersion>(_GetVersion_QNAME, GetVersion.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link AccessDeniedException }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://attachments.bigarchive.magnetosoft.ru/", name = "AccessDeniedException")
    public JAXBElement<AccessDeniedException> createAccessDeniedException(AccessDeniedException value) {
        return new JAXBElement<AccessDeniedException>(_AccessDeniedException_QNAME, AccessDeniedException.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link CreateAttachment }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://attachments.bigarchive.magnetosoft.ru/", name = "createAttachment")
    public JAXBElement<CreateAttachment> createCreateAttachment(CreateAttachment value) {
        return new JAXBElement<CreateAttachment>(_CreateAttachment_QNAME, CreateAttachment.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link BigArchiveServerException }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://attachments.bigarchive.magnetosoft.ru/", name = "BigArchiveServerException")
    public JAXBElement<BigArchiveServerException> createBigArchiveServerException(BigArchiveServerException value) {
        return new JAXBElement<BigArchiveServerException>(_BigArchiveServerException_QNAME, BigArchiveServerException.class, null, value);
    }

}
