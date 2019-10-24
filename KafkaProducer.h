
#pragma once

#include <memory>
#include <rdkafkacpp.h>
#include <string>
#include <csignal>
#include <iostream>

class DeliveryReportCB : public RdKafka::DeliveryReportCb {
public:
    void dr_cb (RdKafka::Message &message) {
        std::cout << "Message delivery for (" << message.len() << " bytes): " <<
                     message.errstr() << std::endl;
        if (message.key())
            std::cout << "Key: " << *(message.key()) << ";" << std::endl;
    }
};
 
class EventCB : public RdKafka::EventCb {
public:
    void event_cb (RdKafka::Event &event) {
        switch (event.type())
        {
        case RdKafka::Event::EVENT_ERROR:
            std::cerr << "ERROR (" << RdKafka::err2str(event.err()) << "): " <<
                         event.str() << std::endl;
            if (event.err() == RdKafka::ERR__ALL_BROKERS_DOWN)
            {
                bool run = false;
            }
            break;
 
        case RdKafka::Event::EVENT_STATS:
            std::cerr << "\"STATS\": " << event.str() << std::endl;
            break;
 
        case RdKafka::Event::EVENT_LOG:
            fprintf(stderr, "LOG-%i-%s: %s\n",
                    event.severity(), event.fac().c_str(), event.str().c_str());
            break;
 
        default:
            std::cerr << "EVENT " << event.type() <<
                         " (" << RdKafka::err2str(event.err()) << "): " <<
                         event.str() << std::endl;
            break;
        }
    }
};

class KafkaProducer{
public:
    KafkaProducer();
    ~KafkaProducer();

    bool Init(const std::string& host, const int port, bool async, const int maxsize = 0);
    void SetTopic(const std::string& topic_str);
    bool Send(const std::string& topic,const std::string message, const int timeout);
    bool Send(const std::string message, const int timeout);
    bool IsGood() { return m_run; }

private:
    bool m_run{false};
    std::shared_ptr<RdKafka::Conf> m_conf{nullptr};
    std::shared_ptr<RdKafka::Conf> m_tconf{nullptr};
    std::shared_ptr<RdKafka::Producer> m_producer{nullptr};
    std::shared_ptr<RdKafka::Topic> m_topic;
    std::shared_ptr<DeliveryReportCB> m_deliverycb{nullptr};
    std::shared_ptr<EventCB> m_eventcb;
};

KafkaProducer::KafkaProducer()
{
}
KafkaProducer::~KafkaProducer()
{
    while (m_run && m_producer->outq_len()>0)
    {
        std::cerr << "Waiting for " << m_producer->outq_len() << std::endl;
        m_producer->poll(1000);
    }
    RdKafka::wait_destroyed(5000);
}

bool KafkaProducer::Init(const std::string& host, const int port, bool async, const int maxsize)
{
    m_run = true;
    m_conf = std::shared_ptr<RdKafka::Conf>(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    m_tconf = std::shared_ptr<RdKafka::Conf>(RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC));
    if(m_conf == nullptr || m_tconf == nullptr)
    {
        m_run = false;
        return m_run;
    }
    std::string errstr;
    std::string broker(host);
    broker.append(":").append(std::to_string(port));    
    m_conf->set("bootstrap.servers", broker, errstr);
    if(async)
    {
        m_conf->set("producer.type", "async", errstr);
        m_conf->set("queue.buffering.max.messages", std::to_string(maxsize).c_str(), errstr);
    }
    else{
        m_conf->set("producer.type", "sync", errstr);
    }
    
    m_deliverycb = std::shared_ptr<DeliveryReportCB>(new DeliveryReportCB);
    m_eventcb = std::shared_ptr<EventCB>(new EventCB);
    m_conf->set("event_cb", m_eventcb.get(), errstr);
    m_conf->set("dr_cb", m_deliverycb.get(), errstr);

    m_producer = std::shared_ptr<RdKafka::Producer>(RdKafka::Producer::create(m_conf.get(), errstr));
    if(m_producer == nullptr)
    {
        m_run = false;
        return m_run;
    }

    return m_run;
}

void KafkaProducer::SetTopic(const std::string& topic_str)
{
    std::string errstr;
    m_topic = std::shared_ptr<RdKafka::Topic>(RdKafka::Topic::create(m_producer.get(), topic_str,m_tconf.get(), errstr));
}

bool KafkaProducer::Send(const std::string& topicstr, const std::string message, const int timeout)
{
    if(!m_run)
    {
        return false;
    }
    if(topicstr.size() == 0)
    {
        return false;
    }
    SetTopic(topicstr);
    RdKafka::ErrorCode resp = m_producer->produce(m_topic.get(), RdKafka::Topic::PARTITION_UA,
                                RdKafka::Producer::RK_MSG_COPY /* Copy payload */,
                                const_cast<char *>(message.c_str()), message.size(),
                                NULL, NULL);
    if (resp != RdKafka::ERR_NO_ERROR)
    {
        std::cerr << "% Produce failed: " <<
                        RdKafka::err2str(resp) << std::endl;
        return false;
    }
    m_producer->poll(timeout);
    return true;
}

bool KafkaProducer::Send(const std::string message, const int timeout)
{
    if(!m_run)
    {
        return false;
    }

    RdKafka::ErrorCode resp = m_producer->produce(m_topic.get(), RdKafka::Topic::PARTITION_UA,
                                RdKafka::Producer::RK_MSG_COPY /* Copy payload */,
                                const_cast<char *>(message.c_str()), message.size(),
                                NULL, NULL);
    if (resp != RdKafka::ERR_NO_ERROR)
    {
        std::cerr << "% Produce failed: " <<
                        RdKafka::err2str(resp) << std::endl;
        return false;
    }
    m_producer->poll(timeout);
    return true;
}
