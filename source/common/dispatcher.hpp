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
            _handlers.insert(std::make_pair(type,std::make_shared<CallBackT<T>>(handler)));
        }
        void OnMessage(const BaseConnection::ptr& conn,const BaseMessage::ptr& message)
        {

            CallBack::ptr callback;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it=_handlers.find(message->GetMessageType());
                if(it==_handlers.end())
                {
                    LOG_ERROR("错误的消息类型:%s",message->Serialize().c_str());
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


