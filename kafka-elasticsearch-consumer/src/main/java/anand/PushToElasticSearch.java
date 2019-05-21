package anand;

import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.http.AWSRequestSigningApacheInterceptor;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.IOException;

public class PushToElasticSearch {
    String serviceName="";
    String region="";
    String aesEndpoint="";
    String document="";
    String indexingPath="";
    AWSCredentialsProvider credentialsProvider;

    public int pushToElasticSearch(String serviceName,String region,String aesEndpoint,String document,String indexingPath){
        credentialsProvider = new DefaultAWSCredentialsProviderChain();
        this.aesEndpoint=aesEndpoint;
        this.document=document;
        this.indexingPath=indexingPath;
        this.serviceName=serviceName;
        this.region=region;
        RestClient esClient = esClient(serviceName, region);


        // Index a document
        HttpEntity entity = new NStringEntity(document, ContentType.APPLICATION_JSON);
        String id = "1";
        Request request = new Request("POST", indexingPath);
        request.setEntity(entity);
        int statusCode=Integer.MAX_VALUE;
        // Using a String instead of an HttpEntity sets Content-Type to application/json automatically.
        // request.setJsonEntity(sampleDocument);

        try {
            Response response = esClient.performRequest(request);
            statusCode=response.getStatusLine().getStatusCode();
        } catch (IOException e) {
            if(e!=null){
                e.printStackTrace();
            }
        }
        return statusCode;
    }

    // Adds the interceptor to the ES REST client
    public  RestClient esClient(String serviceName, String region) {
        AWS4Signer signer = new AWS4Signer();
        signer.setServiceName(serviceName);
        signer.setRegionName(region);
        HttpRequestInterceptor interceptor = new AWSRequestSigningApacheInterceptor(serviceName, signer, credentialsProvider);
        return RestClient.builder(HttpHost.create(aesEndpoint)).setHttpClientConfigCallback(hacb -> hacb.addInterceptorLast(interceptor)).build();
    }
}
