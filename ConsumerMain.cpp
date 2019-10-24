
#include "KafkaConsumer.h"
#include <csignal>

static bool run = true;
void sigterm(int sig)
{
    run = false;
}

int main(int argc, char const *argv[])
{
    signal(SIGINT, sigterm);
    signal(SIGTERM, sigterm);
    signal(SIGKILL, sigterm);

    KafkaConsumer consumer;
    std::string topic = "test";
    consumer.SetTopic(topic);    
    consumer.Init("localhost", 9092, "101");
    consumer.Recv(1000);
    if(run == false)
    {
        std::cout << "stop consumer" << std::endl;
        consumer.Stop();
    }
    return 0;
}


//g++ -o consumer KafkaConsumer.h ConsumerMain.cpp -I/usr/local/include/librdkafka/ -L/usr/local/include/ -lrdkafka -lrdkafka++ --std=c++11

