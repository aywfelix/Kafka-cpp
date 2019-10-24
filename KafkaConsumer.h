
#pragma once

#include <sstream>
#include <iomanip>
#include <iostream>
#include <algorithm>
#include <iterator>
#include <string>
#include <vector>
#include <memory>
#include <unistd.h>
#include <rdkafkacpp.h>


class EventCB : public RdKafka::EventCb {
public:
    void event_cb (RdKafka::Event &event) {
        switch (event.type())
        {
        case RdKafka::Event::EVENT_ERROR:
        {
            std::cerr << "ERROR (" << RdKafka::err2str(event.err()) << "): " <<
                         event.str() << std::endl;
        }
            break;
        case RdKafka::Event::EVENT_STATS:
        {
            std::cerr << "\"STATS\": " << event.str() << std::endl;
        }
            break;
        case RdKafka::Event::EVENT_LOG:
        {
            fprintf(stderr, "LOG-%i-%s: %s\n",event.severity(), event.fac().c_str(), event.str().c_str());
        }
            break;
        case RdKafka::Event::EVENT_THROTTLE:
        {
            std::cerr << "THROTTLED: " << event.throttle_time() << "ms by " <<
                         event.broker_name() << " id " << (int)event.broker_id() << std::endl;
        }
            break;
        default:
            std::cerr << "EVENT " << event.type() <<
                         " (" << RdKafka::err2str(event.err()) << "): " <<
                         event.str() << std::endl;
            break;
        }
    }
};
 
int msg_consume(std::vector<char> &data, RdKafka::Message* message, void* opaque) {
    int len = -1;
    switch (message->err()) {
    case RdKafka::ERR__TIMED_OUT:
        std::cout << "RdKafka::ERR__TIMED_OUT" << std::endl;
        break;
    case RdKafka::ERR_NO_ERROR:
    {
        if(message->payload())
        {
            len = static_cast<int>(message->len());
            data.resize(len);
            const char *msg = static_cast<const char *>(message->payload());
            memcpy(&data[0], msg, len);    
        }
    }
        break;
 
    case RdKafka::ERR__PARTITION_EOF:
    {
        len = 0;
    }
        break;
 
    case RdKafka::ERR__UNKNOWN_TOPIC:
    case RdKafka::ERR__UNKNOWN_PARTITION:
    default:
        /* Errors */
        std::cerr << "Consume failed: " << message->errstr() << std::endl;
        len = -1;
    }
    return len;
}
 
class KafkaConsumer{
public:
    KafkaConsumer();
    ~KafkaConsumer();

    bool Init(const std::string& host, const int port, const std::string& groupid);
    void Recv(const int timeout = 1000);

    void SetTopic(const std::string &topic);
    void SetTopic(const std::vector<std::string> &topics);

    void Stop()
    {
        m_run = false;
    }
private:
    std::vector<std::string> m_topics;
    std::shared_ptr<RdKafka::Conf> m_conf{nullptr};
    std::shared_ptr<RdKafka::Conf> m_tconf{nullptr};
    std::shared_ptr<RdKafka::KafkaConsumer> m_consumer{nullptr};
    bool m_run{false};
    std::shared_ptr<EventCB> m_eventcb{nullptr};
};

KafkaConsumer::KafkaConsumer()
{
}

KafkaConsumer::~KafkaConsumer()
{
    RdKafka::wait_destroyed(5000);    
}

void KafkaConsumer::SetTopic(const std::string &topic)
{
    auto it = std::find(m_topics.begin(), m_topics.end(), topic);
    if(it == m_topics.end())
    {
        m_topics.emplace_back(topic);
    }
}

void KafkaConsumer::SetTopic(const std::vector<std::string> &topics)
{
    m_topics.clear();
    m_topics.assign(topics.begin(), topics.end());
}


bool KafkaConsumer::Init(const std::string& host, const int port, const std::string& groupid)
{
    m_conf = std::shared_ptr<RdKafka::Conf>(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    m_tconf = std::shared_ptr<RdKafka::Conf>(RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC));
    if(m_conf == nullptr || m_tconf == nullptr)
    {
        m_run = false;
        return m_run;
    }
    
    std::string errstr;
    //必须设置
    RdKafka::Conf::ConfResult ret = m_conf->set("group.id", groupid, errstr);
    if (ret != RdKafka::Conf::CONF_OK) {
        m_run = false;
        return m_run;
    }

    std::string broker(host);
    broker.append(":").append(std::to_string(port));
    std::cout << broker << std::endl;
    m_conf->set("bootstrap.servers", broker, errstr);    
    m_conf->set("max.partition.fetch.bytes", "102400", errstr);

    m_tconf->set("auto.offset.reset", "latest", errstr);

    m_eventcb = std::shared_ptr<EventCB>(new EventCB);
    m_conf->set("event_cb", m_eventcb.get(), errstr);
    m_conf->set("default_topic_conf", m_tconf.get(), errstr);

    m_consumer = std::shared_ptr<RdKafka::KafkaConsumer>(RdKafka::KafkaConsumer::create(m_conf.get(), errstr));
    if(m_consumer == nullptr)
    {
        m_run = false;
        return m_run; 
    }

    std::vector<std::string> topics;
    topics.assign(m_topics.begin(), m_topics.end());
    //指定每个topic的每个分区开始消费的位置
    //基本思路为先获取server端的状态信息，将与订阅相关的topic找出来，根据分区，创建TopicPartion；最后使用assign消费
	RdKafka::Metadata *metadataMap{ nullptr };
	RdKafka::ErrorCode err = m_consumer->metadata(true, nullptr, &metadataMap, 2000);
	if (err != RdKafka::ERR_NO_ERROR) {
		std::cout << RdKafka::err2str(err) << std::endl;
	}
	const RdKafka::Metadata::TopicMetadataVector *topicList = metadataMap->topics();
	std::cout << "broker topic size: " << topicList->size() << std::endl;
	RdKafka::Metadata::TopicMetadataVector subTopicMetaVec;
	std::copy_if(topicList->begin(), topicList->end(), std::back_inserter(subTopicMetaVec), [&topics](const RdKafka::TopicMetadata* data) {
		return std::find_if(topics.begin(), topics.end(), [data](const std::string &tname) {return data->topic() == tname; }) != topics.end();
	});
	std::vector<RdKafka::TopicPartition*> topicpartions;
	std::for_each(subTopicMetaVec.begin(), subTopicMetaVec.end(), [&topicpartions](const RdKafka::TopicMetadata* data) {
		auto parVec = data->partitions();
		std::for_each(parVec->begin(), parVec->end(), [&](const RdKafka::PartitionMetadata *value) {
			std::cout << data->topic() << " has partion: " << value->id() << " Leader is : " << value->leader() << std::endl;
			topicpartions.push_back(RdKafka::TopicPartition::create(data->topic(), value->id(), RdKafka::Topic::OFFSET_END));
		});
	});
	m_consumer->assign(topicpartions);    

    m_run = true;
    return true;
}

void KafkaConsumer::Recv(const int timeout)
{
    while(m_run)
    {
        std::vector<char> data;
        std::shared_ptr<RdKafka::Message> msg = std::shared_ptr<RdKafka::Message>(m_consumer->consume(timeout));
        int len = msg_consume(data, msg.get(), NULL);
        if(len > 0)
        {
            std::cout << data.data() << std::endl;
        }
    }
}