#pragma once
#include<unordered_map>
#include"net.hpp"
#include"fields.hpp"

namespace JsonRpc
{
    class CallBack
    {
    public:
        using ptr=std::shared_ptr<CallBack>;
        virtual void OnMessage(const BaseConnection::ptr&,const BaseMessage::ptr&)=0;
    };

    template<class T>
    class CallBackT:public CallBack
    {
    public:
        using ptr=std::shared_ptr<CallBackT>;
        using MessageCallBack=std::function<void(const BaseConnection::ptr&,const std::shared_ptr<T>&)>;
        CallBackT(const MessageCallBack& cb):_cb(cb){}
        virtual void OnMessage(const BaseConnection::ptr& conn,const BaseMessage::ptr& message)override
        {
            auto type_message=std::dynamic_pointer_cast<T>(message);
            if(!type_message)
            {
                LOG_ERROR("消息处理器类型不匹配: message_type=%d message_id=%s",(int)message->GetMessageType(),message->GetMessageId().c_str());
            }
            if(_cb)_cb(conn,type_message);
        }
    private:
        MessageCallBack _cb;
    };

    class Dispatcher
    {
    public:
        using ptr=std::shared_ptr<Dispatcher>;
        template<class T>
        void RegisterHandler(const MessageType& type,const typename CallBackT<T>::MessageCallBack& handler)
        {
            std::unique_lock<std::mutex> lock(_mutex);
            auto ret=_handlers.insert(std::make_pair(type,std::make_shared<CallBackT<T>>(handler)));
            if(!ret.second)
            {
                LOG_WARN("消息处理器重复注册，保留原有处理器: message_type=%d",(int)type);
            }
        }
        void OnMessage(const BaseConnection::ptr& conn,const BaseMessage::ptr& message)
        {

            CallBack::ptr callback;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it=_handlers.find(message->GetMessageType());
                if(it==_handlers.end())
                {
                    LOG_WARN("消息没有对应的处理器: message_type=%d message_id=%s",(int)message->GetMessageType(),message->GetMessageId().c_str());
                    return;
                }
                callback=it->second;
            }
           callback->OnMessage(conn,message);
        }
    private:
        std::mutex _mutex;
        std::unordered_map<MessageType,CallBack::ptr> _handlers;
    };
}
