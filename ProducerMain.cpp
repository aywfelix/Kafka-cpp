
#include "KafkaProducer.h"

int main(int argc, char const *argv[])
{
    KafkaProducer producer;
    producer.Init("localhost", 9092, true, 1000);
    if(producer.IsGood())
    {
        std::string topic = "test";
        std::string message = "I love you";
        producer.Send(topic,message, 1000);
    }
    return 0;
}

//g++ -o producer KafkaProducer.h ProducerMain.cpp -I/usr/local/include/librdkafka/ -L/usr/local/include/ -lrdkafka -lrdkafka++