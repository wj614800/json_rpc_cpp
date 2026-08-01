#pragma once
#include"../common/net.hpp"
#include"requestor.hpp"


namespace JsonRpc
{
    namespace Client
    {
        class TopicManager
        {
        public:
            using ptr=std::shared_ptr<TopicManager>;
            using PublishCallBack=std::function<void(const std::string&,const std::string&)>;
            TopicManager(const Requestor::ptr& requestor):_requestor(requestor)
            {}
            bool Create(const BaseConnection::ptr& conn,const std::string& topic_key)
            {
                return CommonRequest(conn,TopicOptype::TOPIC_CREATE,topic_key);
            }
            bool Remove(const BaseConnection::ptr& conn,const std::string& topic_key)
            {
                return CommonRequest(conn,TopicOptype::TOPIC_REMOVE,topic_key);
            }
            bool Cancel(const BaseConnection::ptr& conn,const std::string& topic_key)
            {
                bool ret=CommonRequest(conn,TopicOptype::TOPIC_CANCEL,topic_key);
                if(ret)
                {
                    RemoveSubscribe(topic_key);
                    return true;
                }
                return false;
            }
            bool Subscribe(const BaseConnection::ptr& conn,const std::string& topic_key,const PublishCallBack& cb)
            {
                AddSubscribe(topic_key,cb);
                bool ret=CommonRequest(conn,TopicOptype::TOPIC_SUBSCRIBE,topic_key);
                if(!ret)
                {
                    RemoveSubscribe(topic_key);
                    return false;
                }
                return true;
            }
            bool Publish(const BaseConnection::ptr& conn,const std::string& topic_key,const std::string& msg)
            {
                return CommonRequest(conn,TopicOptype::TOPIC_PUBLISH,topic_key,msg);
            }
            void OnPublish(const BaseConnection::ptr& conn,const TopicRequest::ptr& request)
            {
                if(request->GetTopickOptype()!=TopicOptype::TOPIC_PUBLISH)
                {
                    LOG_ERROR("主题请求类型错误");
                    return;
                }
                std::string topic_key=request->GetTopickKey();
                std::string topic_msg=request->GetTopickMsg();
                PublishCallBack callback=GetSubscribe(topic_key);
                if(!callback)
                {
                    LOG_ERROR("没有%s主题的回调处理函数",topic_key.c_str());
                    return;
                }
                callback(topic_key,topic_msg);
            }
        private:
            bool CommonRequest(const BaseConnection::ptr& conn,const TopicOptype& type,const std::string& topic_key,const std::string& msg="")
            {
                auto request=MessageFactory::Create<TopicRequest>();
                request->SetMessageId(Util::UUID());
                request->SetMessageType(MessageType::REQUEST_TOPIC);
                request->SetTopicKey(topic_key);
                request->SetTopicOptype(type);
                if(type==TopicOptype::TOPIC_PUBLISH)
                {
                    request->SetTopicMsg(msg);
                }
                BaseMessage::ptr response;
                bool ret=_requestor->Send(conn,request,response);
                if(!ret)
                {
                    LOG_ERROR("主题请求发送失败");
                    return false;
                }
                TopicResponse::ptr topic_response=std::dynamic_pointer_cast<TopicResponse>(response);
                if(!topic_response)
                {
                    LOG_ERROR("主题响应接受错误");
                    return false;
                }

                if(topic_response->GetResponseCode()!=ResponseCode::RCODE_OK)
                {
                    LOG_ERROR("主题响应结果错误：%s",Util::ErrorReason(topic_response->GetResponseCode()).c_str());
                    return false;
                }

                return true;
            }
            void AddSubscribe(const std::string& key,const PublishCallBack& cb)
            {
                std::unique_lock<std::mutex> lock(_mutex);
                _topic_callbacks.insert({key,cb});
            }
            void RemoveSubscribe(const std::string& key)
            {
                std::unique_lock<std::mutex> lock(_mutex);
                _topic_callbacks.erase(key);
            }
            PublishCallBack GetSubscribe(const std::string& key)
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it=_topic_callbacks.find(key);
                if(it==_topic_callbacks.end())
                {
                    return PublishCallBack();
                }
                return it->second;
            }
        private:
            Requestor::ptr _requestor;
            std::mutex _mutex;
            std::unordered_map<std::string,PublishCallBack> _topic_callbacks;
        };
    }
}