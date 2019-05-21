package anand;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class Test {
    public static void main(String[] args) throws IOException {
        /*
        String serviceName="es";
        String region="us-east-1";
        String aesEndpoint="https://search-consumerdomain-htmw4gdz6zi2ju5siotyedafeu.us-east-1.es.amazonaws.com";
        String document="{" + "\"title\":\"Walk the Line\"," + "\"director\":\"James Mangold\"," + "\"year\":\"2005\"}";
        String indexingPath="/my-index/_doc";
        PushToElasticSearch pushToElasticSearch=new PushToElasticSearch();
        pushToElasticSearch.pushToElasticSearch(serviceName,region,aesEndpoint,document,indexingPath);


         */
        //Fetch Connection configuration from config for AWS ElasticSearch
        Properties esProperties=new Properties();
        FileReader reader= null;

            reader = new FileReader("D:\\OneDrive\\Projects\\ApacheKafka\\IDEAProjects\\starter\\kafka-elasticsearch-consumer\\src\\main\\resources\\properties\\awsconfig.properties");
            esProperties.load(reader);
            System.out.println("Here");
            System.out.println("ServiceName:"+esProperties.getProperty("serviceName"));
            System.out.println("Region"+esProperties.getProperty("region"));
            System.out.println("AESEndpoint"+esProperties.getProperty("aesEndpoint"));




    }
}
