#pragma once
#include"fields.hpp"
#include"detail.hpp"
#include"abstract.hpp"

namespace JsonRpc
{
    class JsonMessage:public BaseMessage
    {
    public:
        JsonMessage(){}
        JsonMessage(const MessageType& type):BaseMessage(type){}
        virtual std::string Serialize()override
        {
            std::string body;
            if(!Util::Serialize(_message,body))
            {
                LOG_ERROR("JsonMessage序列化失败");
                return std::string();
            }
            return body;
        }

        virtual bool UnSerialize(const std::string& body)override
        {
            if(!Util::UnSerialize(body,_message))
            {
                LOG_ERROR("JsonMessage反序列化失败");
                return false;
            }
            return true;
        }
    protected:
        Json::Value _message;
    };

    class JsonRequest:public JsonMessage
    {
    public:
        JsonRequest(){}
        JsonRequest(const MessageType& type):JsonMessage(type){}
    };

    class JsonResponse:public JsonMessage
    {
    public:
        JsonResponse(){}
        JsonResponse(const MessageType& type):JsonMessage(type){}
        virtual bool Check()override
        {
            if(_message[KEY_RESPONSE_CODE].isNull()||!_message[KEY_RESPONSE_CODE].isIntegral())
            {
                LOG_ERROR("JsonResponse没有响应状态码字段或字段类型不正确");
                return false;
            }
            return true;
        }

        virtual ResponseCode GetResponseCode()const
        {
            return (ResponseCode)_message[KEY_RESPONSE_CODE].asInt();
        }

        virtual void SetResponseCode(const ResponseCode& code)
        {
            _message[KEY_RESPONSE_CODE]=(int)code;
        }
    };

    class RpcRequest:public JsonRequest
    {
        friend class MessageFactory;
    private:
        RpcRequest():JsonRequest(MessageType::REQUEST_RPC)
        {}
    public:
        using ptr=std::shared_ptr<RpcRequest>;

        virtual bool Check()override
        {
            if(_message[KEY_METHOD].isNull()||!_message[KEY_METHOD].isString())
            {
                LOG_ERROR("Rpc请求没有方法字段或者方法字段类型错误");
                return false;
            }
             if(_message[KEY_PARAMS].isNull()||!_message[KEY_PARAMS].isObject())
            {
                LOG_ERROR("Rpc请求没有参数字段或者参数字段类型错误");
                return false;
            }
            return true;
        }

        std::string GetMethod()const
        {
            return _message[KEY_METHOD].asString();
        }

        void SetMethod(const std::string& method)
        {
            _message[KEY_METHOD]=method;
        }

        Json::Value GetParams()const
        {
            return _message[KEY_PARAMS];
        }

        void SetParams(const Json::Value& params)
        {
            _message[KEY_PARAMS]=params;
        }
    };

    class TopicRequest:public JsonRequest
    {
    private:
        friend class MessageFactory;
        TopicRequest():JsonRequest(MessageType::REQUEST_TOPIC){}
    public:
        using ptr=std::shared_ptr<TopicRequest>;
        virtual bool Check()override
        {
            if(_message[KEY_TOPIC_KEY].isNull()||!_message[KEY_TOPIC_KEY].isString())
            {
                LOG_ERROR("主题请求没有主题名字段或者主题名称字段类型错误");
                return false;
            }
            if(_message[KEY_OPTYPE].isNull()||!_message[KEY_OPTYPE].isInt())
            {
                LOG_ERROR("主题请求没有操作类型字段或者操作类型字段类型错误");
                return false;
            }
            if(_message[KEY_OPTYPE].asInt()==(int)TopicOptype::TOPIC_PUBLISH&&(_message[KEY_TOPIC_MSG].isNull()||!_message[KEY_TOPIC_MSG].isString()))
            {
                LOG_ERROR("主题发布的主题请求没有主题消息字段或者主题消息字段类型错误");
                return false;
            }
            return true;
        }

        std::string GetTopickKey()const
        {
            return _message[KEY_TOPIC_KEY].asString();
        }

        void SetTopicKey(const std::string& topicKey)
        {
            _message[KEY_TOPIC_KEY]=topicKey;
        }

        TopicOptype GetTopickOptype()const
        {
            return (TopicOptype)_message[KEY_OPTYPE].asInt();
        }

        void SetTopicOptype(const TopicOptype& optype)
        {
            _message[KEY_OPTYPE]=(int)optype;
        }

        std::string GetTopickMsg()const
        {
            return _message[KEY_TOPIC_MSG].asString();
        }

        void SetTopicMsg(const std::string& msg)
        {
            _message[KEY_TOPIC_MSG]=msg;
        }
    };

    using Address=std::pair<std::string,int>;
    class ServiceRequest:public JsonRequest
    {
    private:
        friend class MessageFactory;
        ServiceRequest():JsonRequest(MessageType::REQUEST_SERVICE){}
    public:
        using ptr=std::shared_ptr<ServiceRequest>;
        virtual bool Check()override
        {
           if(_message[KEY_METHOD].isNull()||!_message[KEY_METHOD].isString())
            {
                LOG_ERROR("服务请求没有方法字段或者方法字段类型错误");
                return false;
            }
            if(_message[KEY_OPTYPE].isNull()||!_message[KEY_OPTYPE].isInt())
            {
                LOG_ERROR("服务请求没有操作类型字段或者操作类型字段类型错误");
                return false;
            }
            if(_message[KEY_OPTYPE].asInt()==(int)ServiceOptype::SERVICE_REGISTER
            &&(_message[KEY_HOST].isNull()||!_message[KEY_HOST].isObject()
                ||_message[KEY_HOST][KEY_HOST_IP].isNull()
                ||!_message[KEY_HOST][KEY_HOST_IP].isString()
                ||_message[KEY_HOST][KEY_HOST_PORT].isNull()
                ||!_message[KEY_HOST][KEY_HOST_PORT].isInt()))
            {
                LOG_ERROR("服务注册的服务请求中ip和port字段缺失或者类型错误");
                return false;
            }
            return true;
        }

        std::string GetMethod()const
        {
            return _message[KEY_METHOD].asString();
        }

        void SetMethod(const std::string& method)
        {
            _message[KEY_METHOD]=method;
        }

        ServiceOptype GetServiceOptype()const
        {
            return (ServiceOptype)_message[KEY_OPTYPE].asInt();
        }

        void SetServiceOptype(const ServiceOptype& optype)
        {
            _message[KEY_OPTYPE]=(int)optype;
        }

        Address GetHost()const
        {
            Address address;
            address.first=_message[KEY_HOST][KEY_HOST_IP].asString();
            address.second=_message[KEY_HOST][KEY_HOST_PORT].asInt();
            return address;
        }

        void SetHost(const Address& address)
        {
            _message[KEY_HOST][KEY_HOST_IP]=address.first;
            _message[KEY_HOST][KEY_HOST_PORT]=address.second;
        }
    };

    class RpcResponse:public JsonResponse
    {
    private:
        friend class MessageFactory;
        RpcResponse():JsonResponse(MessageType::RESPONSE_RPC){}
    public:
        using ptr=std::shared_ptr<RpcResponse>;
        virtual bool Check()override
        {
            if(_message[KEY_RESPONSE_CODE].isNull()||!_message[KEY_RESPONSE_CODE].isInt())
            {
                LOG_ERROR("Rpc响应没有响应状态码字段或字段类型错误");
                return false;
            }

            if(_message[KEY_RESPONSE_CODE].asInt()==(int)ResponseCode::RCODE_OK&&_message[KEY_RESPONSE_RESULT].isNull())
            {
                LOG_ERROR("Rpc响应字段响应状态码正确但是没有结果字段");
                return false;
            }

            return true;
        }

        Json::Value GetResponseResult()const
        {
            return _message[KEY_RESPONSE_RESULT];
        }

        void SetResponseResult(const Json::Value& result)
        {
            _message[KEY_RESPONSE_RESULT]=result;
        }
    };


    class TopicResponse:public JsonResponse
    {
    private:
        friend class MessageFactory;
        TopicResponse():JsonResponse(MessageType::RESPONSE_TOPIC){}
    public:
        using ptr=std::shared_ptr<TopicResponse>;
    };

    class ServiceResponse:public JsonResponse
    {
     private:
        friend class MessageFactory;
        ServiceResponse():JsonResponse(MessageType::RESPONSE_SERVICE){}
    public:
        using ptr=std::shared_ptr<ServiceResponse>;
        virtual bool Check()override
        {
            if(_message[KEY_RESPONSE_CODE].isNull()||!_message[KEY_RESPONSE_CODE].isInt())
            {
                LOG_ERROR("没有响应状态码字段或字段类型不正确");
                return false;
            }
            if(_message[KEY_OPTYPE].isNull()||!_message[KEY_OPTYPE].isInt())
            {
                LOG_ERROR("没有操作类型字段或字段类型不正确");
                return false;
            }
            if(_message[KEY_OPTYPE].asInt()==(int)ServiceOptype::SERVICE_DISCOVER)
            {
                if(_message[KEY_METHOD].isNull()||!_message[KEY_METHOD].isString())
                {
                    LOG_ERROR("没有method字段或者method字段不正确");
                    return false;
                }
                if(_message[KEY_HOST].isNull()||!_message[KEY_HOST].isArray())
                {
                    LOG_ERROR("没有host字段或者字段类型不正确");
                    return false;
                }
                for(int i=0;i<_message[KEY_HOST].size();i++)
                {
                    if(_message[KEY_HOST][i][KEY_HOST_IP].isNull()||!_message[KEY_HOST][i][KEY_HOST_IP].isString())
                    {
                        LOG_ERROR("没有KEY_HOST_IP字段或者字段类型不正确");
                        return false;
                    }
                    if(_message[KEY_HOST][i][KEY_HOST_PORT].isNull()||!_message[KEY_HOST][i][KEY_HOST_PORT].isInt())
                    {
                        LOG_ERROR("没有KEY_HOST_PORT字段或者字段类型不正确");
                        return false;
                    }
                }
            }
            return true;
        }

        ServiceOptype GetServiceOptype()const
        {
            return (ServiceOptype)_message[KEY_OPTYPE].asInt();
        }

        void SetServiceOptype(const ServiceOptype& optype)
        {
            _message[KEY_OPTYPE]=(int)optype;
        }

        std::string GetMethod()const
        {
            return _message[KEY_METHOD].asString();
        }

        void SetMethod(const std::string& method)
        {
            _message[KEY_METHOD]=method;
        }

        std::vector<Address> GetHosts()const
        {
            std::vector<Address> hosts;
            for(int i=0;i<_message[KEY_HOST].size();i++)
            {
                hosts.emplace_back(_message[KEY_HOST][i][KEY_HOST_IP].asString(),_message[KEY_HOST][i][KEY_HOST_PORT].asInt());
            }
            return hosts;
        }

        void SetHosts(const std::vector<Address>& hosts)
        {
            if(!_message[KEY_HOST].isArray()) 
            {
                _message[KEY_HOST]=Json::arrayValue;
            }
    
            for(auto& host:hosts)
            {
                Json::Value item;
                item[KEY_HOST_IP]=host.first;
                item[KEY_HOST_PORT]=host.second;
                _message[KEY_HOST].append(item);
            }
        }
    };

    class MessageFactory
    {
    public:
        static BaseMessage::ptr Create(const MessageType& type)
        {
            switch(type)
            {
                case MessageType::REQUEST_RPC:
                    return std::shared_ptr<RpcRequest>(new RpcRequest);
                case MessageType::REQUEST_TOPIC:
                    return std::shared_ptr<TopicRequest>(new TopicRequest);
                case MessageType::REQUEST_SERVICE:
                    return std::shared_ptr<ServiceRequest>(new ServiceRequest);
                case MessageType::RESPONSE_RPC:
                    return std::shared_ptr<RpcResponse>(new RpcResponse);
                case MessageType::RESPONSE_TOPIC:
                    return std::shared_ptr<TopicResponse>(new TopicResponse);
                case MessageType::RESPONSE_SERVICE:
                    return std::shared_ptr<ServiceResponse>(new ServiceResponse);
            }
            return BaseMessage::ptr();
        }
        template<class T,class ...Args>
        static std::shared_ptr<T> Create(Args&& ...args)
        {
            return std::shared_ptr<T>(new T(std::forward<Args>(args)...));
        }
    };
}