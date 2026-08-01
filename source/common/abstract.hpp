#pragma once
#include"fields.hpp"
#include<string>
#include<memory>
#include<functional>
namespace JsonRpc
{
    class BaseMessage
    {
    public:
        using ptr=std::shared_ptr<BaseMessage>;
        BaseMessage(const MessageType& type):_message_type(type)
        {}
        BaseMessage()
        {}
        virtual ~BaseMessage(){}
        virtual void SetMessageType(const MessageType& type)
        {
            _message_type=type;
        }

        virtual MessageType GetMessageType()const
        {
            return _message_type;
        }

        virtual void SetMessageId(const std::string& id)
        {
            _message_id=id;
        }

        virtual std::string GetMessageId()const
        {
            return _message_id;
        }

        virtual std::string Serialize()=0;
        virtual bool UnSerialize(const std::string& message)=0;
        virtual bool Check()=0;
    private:
        MessageType _message_type;
        std::string _message_id;
    };

    class BaseBuffer
    {
    public:
        using ptr=std::shared_ptr<BaseBuffer>;
        virtual ~BaseBuffer(){}
        virtual size_t ReadAbleBytes()const=0;
        virtual int32_t PeekInt32()=0;
        virtual void RetrieveInt32()=0;
        virtual int32_t ReadInt32()=0;
        virtual std::string ReadAsString(size_t n)=0;
    };


    class BaseProtocol
    {
    public:
        using ptr=std::shared_ptr<BaseProtocol>;
        virtual ~BaseProtocol(){}
        virtual bool CanProcess(const BaseBuffer::ptr&)=0;
        virtual bool OnMessage(const BaseBuffer::ptr&,BaseMessage::ptr&)=0;
        virtual std::string Serialize(const BaseMessage::ptr&)=0;
    };


    class BaseConnection
    {
    public:
        using ptr=std::shared_ptr<BaseConnection>;
        virtual ~BaseConnection(){}
        virtual void Send(const BaseMessage::ptr&)=0;
        virtual void Shutdown()=0;
        virtual bool Connected()=0;
    };


    using ConnectCallBack=std::function<void(const BaseConnection::ptr&)>;
    using MessageCallBack=std::function<void(const BaseConnection::ptr&,const BaseMessage::ptr&)>;
    using CloseCallBack=std::function<void(const BaseConnection::ptr&)>;
    class BaseServer
    {
    public:
        using ptr=std::shared_ptr<BaseServer>;
        void SetConnectCallBack(const ConnectCallBack& cb)
        {
            _connect_cb=cb;
        }
        void SetMessageCallBack(const MessageCallBack& cb)
        {
            _message_cb=cb;
        }
        void SetCloseCallBack(const CloseCallBack& cb)
        {
            _close_cb=cb;
        }
        virtual void Start()=0;
    protected:
        ConnectCallBack _connect_cb;
        MessageCallBack _message_cb;
        CloseCallBack _close_cb;
    };

    class BaseClient
    {
    public:
        using ptr=std::shared_ptr<BaseClient>;
        virtual ~BaseClient(){}
        void SetConnectCallBack(const ConnectCallBack& cb)
        {
            _connect_cb=cb;
        }
        void SetMessageCallBack(const MessageCallBack& cb)
        {
            _message_cb=cb;
        }
        void SetCloseCallBack(const CloseCallBack& cb)
        {
            _close_cb=cb;
        }
        virtual bool Connect()=0;
        virtual bool Connected()=0;
        virtual void Shutdown()=0;
        virtual void Send(const BaseMessage::ptr&)=0;
        virtual BaseConnection::ptr Connection()=0;
    protected:
        ConnectCallBack _connect_cb;
        MessageCallBack _message_cb;
        CloseCallBack _close_cb;
    };
}