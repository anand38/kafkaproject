package anand;

import com.amazonaws.auth.*;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import com.amazonaws.http.AWSRequestSigningApacheInterceptor;
import java.io.IOException;

public class AmazonElasticsearchServiceSample {

    private static String serviceName = "es";
    private static String region = "us-east-1";
    private static String aesEndpoint = "https://search-consumerdomain-htmw4gdz6zi2ju5siotyedafeu.us-east-1.es.amazonaws.com";

    private static String payload = "{ \"type\": \"value\"}";
    private static String snapshotPath = "/_snapshot/my-snapshot-repo";

    private static String sampleDocument = "{" + "\"title\":\"Walk the Line\"," + "\"director\":\"James Mangold\"," + "\"year\":\"2005\"}";
    private static String indexingPath = "/my-index/_doc";

    static final AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();
    public static void main(String[] args) throws IOException {
        RestClient esClient = esClient(serviceName, region);
        // Register a snapshot repository
        HttpEntity entity = new NStringEntity(payload, ContentType.APPLICATION_JSON);

        // Index a document
        entity = new NStringEntity(sampleDocument, ContentType.APPLICATION_JSON);
        String id = "1";
        Request request = new Request("POST", indexingPath);
        request.setEntity(entity);

        // Using a String instead of an HttpEntity sets Content-Type to application/json automatically.
        // request.setJsonEntity(sampleDocument);

        Response response = esClient.performRequest(request);
        System.out.println(response.toString());
    }

    // Adds the interceptor to the ES REST client
    public static RestClient esClient(String serviceName, String region) {
        AWS4Signer signer = new AWS4Signer();
        signer.setServiceName(serviceName);
        signer.setRegionName(region);
        HttpRequestInterceptor interceptor = new AWSRequestSigningApacheInterceptor(serviceName, signer, credentialsProvider);
        return RestClient.builder(HttpHost.create(aesEndpoint)).setHttpClientConfigCallback(hacb -> hacb.addInterceptorLast(interceptor)).build();
    }
}