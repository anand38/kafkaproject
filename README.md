# kafkaproject

This is a maven project in which we have producer producing data to Kafka topic from stream of data from twitter and also have AWS Elasticsearch consumer for consuming data from Kafka topic.

Note:
You need to make "awsconfig.properties" and "twitterconfig.properties" in "kafka-elasticsearch-consumer" and "kafka-twitter-producer" module respectively under resources.

Sample awsconfig.properties

serviceName=es
region=us-east-1
aesEndpoint=some endpoint


Sample twitterconfig.properties

consumerKey=your key
consumerSecret=your secret
apiToken=your apiToken
apiSecret=your apiSecret
