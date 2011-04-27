
package magnetico.ws.search;

import javax.xml.bind.JAXBElement;
import javax.xml.bind.annotation.XmlElementDecl;
import javax.xml.bind.annotation.XmlRegistry;
import javax.xml.namespace.QName;


/**
 * This object contains factory methods for each 
 * Java content interface and Java element interface 
 * generated in the magnetico.ws.search package. 
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

    private final static QName _SearchResultResponse_QNAME = new QName("http://search.bigarchive.magnetosoft.ru/", "searchResultResponse");
    private final static QName _BigArchiveServerException_QNAME = new QName("http://search.bigarchive.magnetosoft.ru/", "BigArchiveServerException");
    private final static QName _MapData_QNAME = new QName("http://search.bigarchive.magnetosoft.ru/", "mapData");
    private final static QName _AccessDeniedException_QNAME = new QName("http://search.bigarchive.magnetosoft.ru/", "AccessDeniedException");
    private final static QName _SearchAsync_QNAME = new QName("http://search.bigarchive.magnetosoft.ru/", "searchAsync");
    private final static QName _SearchAsyncResponse_QNAME = new QName("http://search.bigarchive.magnetosoft.ru/", "searchAsyncResponse");
    private final static QName _SearchSyncResponse_QNAME = new QName("http://search.bigarchive.magnetosoft.ru/", "searchSyncResponse");
    private final static QName _SearchResultsResponse_QNAME = new QName("http://search.bigarchive.magnetosoft.ru/", "searchResultsResponse");
    private final static QName _SearchResultsRequest_QNAME = new QName("http://search.bigarchive.magnetosoft.ru/", "searchResultsRequest");
    private final static QName _IsSearchReady_QNAME = new QName("http://search.bigarchive.magnetosoft.ru/", "isSearchReady");
    private final static QName _GetSearchResultsResponse_QNAME = new QName("http://search.bigarchive.magnetosoft.ru/", "getSearchResultsResponse");
    private final static QName _SearchSync_QNAME = new QName("http://search.bigarchive.magnetosoft.ru/", "searchSync");
    private final static QName _GetSearchResults_QNAME = new QName("http://search.bigarchive.magnetosoft.ru/", "getSearchResults");
    private final static QName _SearchResponse_QNAME = new QName("http://search.bigarchive.magnetosoft.ru/", "searchResponse");
    private final static QName _SearchRequest_QNAME = new QName("http://search.bigarchive.magnetosoft.ru/", "searchRequest");
    private final static QName _IsSearchReadyResponse_QNAME = new QName("http://search.bigarchive.magnetosoft.ru/", "isSearchReadyResponse");

    /**
     * Create a new ObjectFactory that can be used to create new instances of schema derived classes for package: magnetico.ws.search
     * 
     */
    public ObjectFactory() {
    }

    /**
     * Create an instance of {@link SearchResponseType }
     * 
     */
    public SearchResponseType createSearchResponseType() {
        return new SearchResponseType();
    }

    /**
     * Create an instance of {@link IsSearchReadyResponse }
     * 
     */
    public IsSearchReadyResponse createIsSearchReadyResponse() {
        return new IsSearchReadyResponse();
    }

    /**
     * Create an instance of {@link MapDataType }
     * 
     */
    public MapDataType createMapDataType() {
        return new MapDataType();
    }

    /**
     * Create an instance of {@link IsSearchReady }
     * 
     */
    public IsSearchReady createIsSearchReady() {
        return new IsSearchReady();
    }

    /**
     * Create an instance of {@link SearchSync }
     * 
     */
    public SearchSync createSearchSync() {
        return new SearchSync();
    }

    /**
     * Create an instance of {@link SearchSyncResponse }
     * 
     */
    public SearchSyncResponse createSearchSyncResponse() {
        return new SearchSyncResponse();
    }

    /**
     * Create an instance of {@link GetSearchResultsResponse }
     * 
     */
    public GetSearchResultsResponse createGetSearchResultsResponse() {
        return new GetSearchResultsResponse();
    }

    /**
     * Create an instance of {@link SearchResultsRequestType }
     * 
     */
    public SearchResultsRequestType createSearchResultsRequestType() {
        return new SearchResultsRequestType();
    }

    /**
     * Create an instance of {@link HashMapType }
     * 
     */
    public HashMapType createHashMapType() {
        return new HashMapType();
    }

    /**
     * Create an instance of {@link SearchResultsResponseType }
     * 
     */
    public SearchResultsResponseType createSearchResultsResponseType() {
        return new SearchResultsResponseType();
    }

    /**
     * Create an instance of {@link BigArchiveServerException }
     * 
     */
    public BigArchiveServerException createBigArchiveServerException() {
        return new BigArchiveServerException();
    }

    /**
     * Create an instance of {@link SearchResultResponseType }
     * 
     */
    public SearchResultResponseType createSearchResultResponseType() {
        return new SearchResultResponseType();
    }

    /**
     * Create an instance of {@link SearchRequestType }
     * 
     */
    public SearchRequestType createSearchRequestType() {
        return new SearchRequestType();
    }

    /**
     * Create an instance of {@link AccessDeniedException }
     * 
     */
    public AccessDeniedException createAccessDeniedException() {
        return new AccessDeniedException();
    }

    /**
     * Create an instance of {@link SearchAsync }
     * 
     */
    public SearchAsync createSearchAsync() {
        return new SearchAsync();
    }

    /**
     * Create an instance of {@link HashMapEntryType }
     * 
     */
    public HashMapEntryType createHashMapEntryType() {
        return new HashMapEntryType();
    }

    /**
     * Create an instance of {@link GetSearchResults }
     * 
     */
    public GetSearchResults createGetSearchResults() {
        return new GetSearchResults();
    }

    /**
     * Create an instance of {@link SearchAsyncResponse }
     * 
     */
    public SearchAsyncResponse createSearchAsyncResponse() {
        return new SearchAsyncResponse();
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link SearchResultResponseType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://search.bigarchive.magnetosoft.ru/", name = "searchResultResponse")
    public JAXBElement<SearchResultResponseType> createSearchResultResponse(SearchResultResponseType value) {
        return new JAXBElement<SearchResultResponseType>(_SearchResultResponse_QNAME, SearchResultResponseType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link BigArchiveServerException }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://search.bigarchive.magnetosoft.ru/", name = "BigArchiveServerException")
    public JAXBElement<BigArchiveServerException> createBigArchiveServerException(BigArchiveServerException value) {
        return new JAXBElement<BigArchiveServerException>(_BigArchiveServerException_QNAME, BigArchiveServerException.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link MapDataType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://search.bigarchive.magnetosoft.ru/", name = "mapData")
    public JAXBElement<MapDataType> createMapData(MapDataType value) {
        return new JAXBElement<MapDataType>(_MapData_QNAME, MapDataType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link AccessDeniedException }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://search.bigarchive.magnetosoft.ru/", name = "AccessDeniedException")
    public JAXBElement<AccessDeniedException> createAccessDeniedException(AccessDeniedException value) {
        return new JAXBElement<AccessDeniedException>(_AccessDeniedException_QNAME, AccessDeniedException.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link SearchAsync }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://search.bigarchive.magnetosoft.ru/", name = "searchAsync")
    public JAXBElement<SearchAsync> createSearchAsync(SearchAsync value) {
        return new JAXBElement<SearchAsync>(_SearchAsync_QNAME, SearchAsync.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link SearchAsyncResponse }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://search.bigarchive.magnetosoft.ru/", name = "searchAsyncResponse")
    public JAXBElement<SearchAsyncResponse> createSearchAsyncResponse(SearchAsyncResponse value) {
        return new JAXBElement<SearchAsyncResponse>(_SearchAsyncResponse_QNAME, SearchAsyncResponse.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link SearchSyncResponse }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://search.bigarchive.magnetosoft.ru/", name = "searchSyncResponse")
    public JAXBElement<SearchSyncResponse> createSearchSyncResponse(SearchSyncResponse value) {
        return new JAXBElement<SearchSyncResponse>(_SearchSyncResponse_QNAME, SearchSyncResponse.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link SearchResultsResponseType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://search.bigarchive.magnetosoft.ru/", name = "searchResultsResponse")
    public JAXBElement<SearchResultsResponseType> createSearchResultsResponse(SearchResultsResponseType value) {
        return new JAXBElement<SearchResultsResponseType>(_SearchResultsResponse_QNAME, SearchResultsResponseType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link SearchResultsRequestType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://search.bigarchive.magnetosoft.ru/", name = "searchResultsRequest")
    public JAXBElement<SearchResultsRequestType> createSearchResultsRequest(SearchResultsRequestType value) {
        return new JAXBElement<SearchResultsRequestType>(_SearchResultsRequest_QNAME, SearchResultsRequestType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link IsSearchReady }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://search.bigarchive.magnetosoft.ru/", name = "isSearchReady")
    public JAXBElement<IsSearchReady> createIsSearchReady(IsSearchReady value) {
        return new JAXBElement<IsSearchReady>(_IsSearchReady_QNAME, IsSearchReady.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link GetSearchResultsResponse }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://search.bigarchive.magnetosoft.ru/", name = "getSearchResultsResponse")
    public JAXBElement<GetSearchResultsResponse> createGetSearchResultsResponse(GetSearchResultsResponse value) {
        return new JAXBElement<GetSearchResultsResponse>(_GetSearchResultsResponse_QNAME, GetSearchResultsResponse.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link SearchSync }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://search.bigarchive.magnetosoft.ru/", name = "searchSync")
    public JAXBElement<SearchSync> createSearchSync(SearchSync value) {
        return new JAXBElement<SearchSync>(_SearchSync_QNAME, SearchSync.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link GetSearchResults }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://search.bigarchive.magnetosoft.ru/", name = "getSearchResults")
    public JAXBElement<GetSearchResults> createGetSearchResults(GetSearchResults value) {
        return new JAXBElement<GetSearchResults>(_GetSearchResults_QNAME, GetSearchResults.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link SearchResponseType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://search.bigarchive.magnetosoft.ru/", name = "searchResponse")
    public JAXBElement<SearchResponseType> createSearchResponse(SearchResponseType value) {
        return new JAXBElement<SearchResponseType>(_SearchResponse_QNAME, SearchResponseType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link SearchRequestType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://search.bigarchive.magnetosoft.ru/", name = "searchRequest")
    public JAXBElement<SearchRequestType> createSearchRequest(SearchRequestType value) {
        return new JAXBElement<SearchRequestType>(_SearchRequest_QNAME, SearchRequestType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link IsSearchReadyResponse }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://search.bigarchive.magnetosoft.ru/", name = "isSearchReadyResponse")
    public JAXBElement<IsSearchReadyResponse> createIsSearchReadyResponse(IsSearchReadyResponse value) {
        return new JAXBElement<IsSearchReadyResponse>(_IsSearchReadyResponse_QNAME, IsSearchReadyResponse.class, null, value);
    }

}
