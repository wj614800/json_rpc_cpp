#pragma once
#include"../common/net.hpp"
#include<unordered_set>
namespace JsonRpc
{
    namespace Server
    {
        class Subscriber
        {
        public:
            using ptr=std::shared_ptr<Subscriber>;
            Subscriber(const BaseConnection::ptr& conn):_conn(conn)
            {}
            void AddTopic(const std::string& topic_name)
            {
                std::unique_lock<std::mutex> lock(_mutex);
                _topics.insert(topic_name);
            }
            void RemoveTopic(const std::string& topic_name)
            {
                std::unique_lock<std::mutex> lock(_mutex);
                _topics.erase(topic_name);
            }
            void Send(const BaseMessage::ptr& message)
            {
                _conn->Send(message);
            }
        private:
            std::mutex _mutex;
            BaseConnection::ptr _conn;
            std::unordered_set<std::string> _topics;
        };


        class Topic
        {
        public:
            using ptr=std::shared_ptr<Topic>;
            Topic(const std::string& topic_name):_topic_name(topic_name)
            {}
            void AddSubscriber(const Subscriber::ptr& subscriber)
            {
                subscriber->AddTopic(_topic_name);
                std::unique_lock<std::mutex> lock(_mutex);
                _subscribers.insert(subscriber);
            }
            void RemoveSubscriber(const Subscriber::ptr& subscriber)
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it=_subscribers.find(subscriber);
                if(it==_subscribers.end())
                {
                    return;
                }
                (*it)->RemoveTopic(_topic_name);
                _subscribers.erase(subscriber);
            }

            void RemoveAllSubscriber()
            {
                std::unique_lock<std::mutex> lock(_mutex);
                for(auto& subscriber:_subscribers)
                {
                    subscriber->RemoveTopic(_topic_name);
                    _subscribers.erase(subscriber);
                }
            }

            void SendSubscribers(const BaseMessage::ptr& message)
            {
                std::unique_lock<std::mutex> lock(_mutex);
                for(auto& subscriber:_subscribers)
                {
                    subscriber->Send(message);
                }
            }
        private:
            std::mutex _mutex;
            std::string _topic_name;
            std::unordered_set<Subscriber::ptr> _subscribers;
        };

        class TopicManager
        {
        public:
            using ptr=std::shared_ptr<TopicManager>;
            TopicManager(){}
            void OnTopicRequest(const BaseConnection::ptr& conn,const TopicRequest::ptr& request)
            {
                bool ret=true;
                switch(request->GetTopickOptype())
                {
                    case TopicOptype::TOPIC_CREATE:AddTopic(request);break;
                    case TopicOptype::TOPIC_REMOVE:RemoveTopic(request);break;
                    case TopicOptype::TOPIC_SUBSCRIBE:ret=SubscribeTopic(conn,request);break;
                    case TopicOptype::TOPIC_CANCEL:CancelSubscirbe(conn,request);break;
                    case TopicOptype::TOPIC_PUBLISH:ret=PublishTopic(conn,request);break;
                    default:Response(conn,request,ResponseCode::RCODE_INVALID_OPTYPE);return;
                }
                if(ret)
                {
                    Response(conn,request,ResponseCode::RCODE_OK);
                }
                else
                {
                    Response(conn,request,ResponseCode::RCODE_NOT_FOUND_TOPIC);
                }
            }
            void OnCloseCallBack(const BaseConnection::ptr& conn)
            {

            }
        private:
            void AddTopic(const TopicRequest::ptr& request)
            {
                std::string topic_name=request->GetTopickKey();
                Topic::ptr topic=std::make_shared<Topic>(topic_name);
                {
                    std::unique_lock<std::mutex> lock(_mutex);
                    _topics.insert({topic_name,topic});
                }
            }
            void RemoveTopic(const TopicRequest::ptr& request)
            {
                std::string topic_name=request->GetTopickKey();
                Topic::ptr topic;
                {
                    std::unique_lock<std::mutex> lock(_mutex);
                    auto it=_topics.find(topic_name);
                    if(it==_topics.end())
                    {
                        return;
                    }
                    topic=it->second;
                    _topics.erase(it);
                }
                topic->RemoveAllSubscriber();
            }
            bool SubscribeTopic(const BaseConnection::ptr& conn,const TopicRequest::ptr& request)
            {
                std::string topic_name=request->GetTopickKey();
                Subscriber::ptr subscriber=std::make_shared<Subscriber>(conn);
                Topic::ptr topic;
                {
                    std::unique_lock<std::mutex> lock(_mutex);
                    auto it=_topics.find(topic_name);
                    if(it==_topics.end())
                    {
                        return false;
                    }
                    topic=it->second;
                    _subscribers.insert({conn,subscriber});
                }
                topic->AddSubscriber(subscriber);
                return true;
            }
            void CancelSubscirbe(const BaseConnection::ptr& conn,const TopicRequest::ptr& request)
            {
                std::string topic_name=request->GetTopickKey();
                Topic::ptr topic;
                Subscriber::ptr subscirber;
                {
                    std::unique_lock<std::mutex> lock(_mutex);
                    auto it=_topics.find(topic_name);
                    if(it==_topics.end())
                    {
                        return;
                    }
                    topic=it->second;
                    auto subscirber_it=_subscribers.find(conn);
                    if(subscirber_it==_subscribers.end())
                    {
                        return;
                    }
                    subscirber=subscirber_it->second;
                }
                topic->RemoveSubscriber(subscirber);
            }
            bool PublishTopic(const BaseConnection::ptr& conn,const TopicRequest::ptr& request)
            {
                std::string topic_name=request->GetTopickKey();
                Topic::ptr topic;
                {
                    std::unique_lock<std::mutex> lock(_mutex);
                    auto it=_topics.find(topic_name);
                    if(it==_topics.end())
                    {
                        return false;
                    }
                    topic=it->second;
                }
                topic->SendSubscribers(request);
                return true;
            }
            void Response(const BaseConnection::ptr& conn,const TopicRequest::ptr& request,const ResponseCode& code)
            {
                TopicResponse::ptr response=MessageFactory::Create<TopicResponse>();
                response->SetMessageId(request->GetMessageId());
                response->SetMessageType(MessageType::RESPONSE_TOPIC);
                response->SetResponseCode(code);
                conn->Send(response);
            }
        private:
            std::mutex _mutex;
            std::unordered_map<std::string,Topic::ptr> _topics;
            std::unordered_map<BaseConnection::ptr,Subscriber::ptr> _subscribers;
        };
    }
}