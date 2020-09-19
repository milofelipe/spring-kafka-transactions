# Read Me First
This is a sample Spring Boot application that shows how to configure Kafka Transactions support.

# Getting Started

You need a running Kafka cluster before starting the application. You can use the one setup in this project. 
On a terminal, run:

`cd kafka/ ; docker-compose up`

Start the application on another terminal:

`./gradlew bootRun`

Monitor topic-a and topic-b:

On another terminal (for topic a)

`cd kafka/`

`docker-compose exec broker kafka-console-consumer --bootstrap-server localhost:9092 --topic topic-a`

On another terminal (for topic b)

`cd kafka/`

`docker-compose exec broker kafka-console-consumer --bootstrap-server localhost:9092 --topic topic-b`

Send messages to topic 'messages'

`cd kafka/`

`docker-compose exec broker kafka-console-producer --broker-list localhost:9092 --topic messages`



