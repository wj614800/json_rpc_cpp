#pragma once
#include<string>
#include"../common/net.hpp"
namespace JsonRpc
{
    namespace Server
    {
        enum class VType
        {
            BOOL=0,
            INT,
            INT64,
            UINT,
            UINT64,
            INTERGRAL,
            DOUBLE,
            STRING,
            OBJECT,
            ARRAY
        };

        class ServiceDescribe
        {
        public:
            using ptr=std::shared_ptr<ServiceDescribe>;
            using ServiceParam=std::pair<std::string,VType>;
            using ServiceCallBack=std::function<void(const Json::Value& ,Json::Value&)>;
            ServiceDescribe(const std::string& method,std::vector<ServiceParam>&& params,VType type,const ServiceCallBack& cb)
                :_method(method)
                ,_service_params(std::move(params))
                ,_return_type(type)
                ,_service_callback(cb)
            {}
            std::string GetMethod()const
            {
                return _method;
            }
            bool CheckParams(const Json::Value& params)
            {
                for(auto& param:_service_params)
                {
                    if(params.isMember(param.first)==false)
                    {
                        LOG_WARN("RPC请求缺少参数: method=%s field=%s",_method.c_str(),param.first.c_str());
                        return false;
                    }
                    if(CheckType(param.second,params[param.first])==false)
                    {
                        LOG_WARN("RPC请求参数类型不匹配: method=%s field=%s expected_type=%d",_method.c_str(),param.first.c_str(),(int)param.second);
                        return false;
                    }
                }
                return true;
            }
           
            bool Caller(const Json::Value& params,Json::Value& result)
            {
                _service_callback(params,result);
                if(!CheckResult(result))
                {
                    LOG_ERROR("RPC服务返回值类型不匹配: method=%s expected_type=%d",_method.c_str(),(int)_return_type);
                    return false;
                }
                return true;
            }
        private:
            bool CheckResult(const Json::Value& result)
            {
                return CheckType(_return_type,result);
            }
            bool CheckType(VType type,const Json::Value& param)
            {
                switch (type)
                {
                    case VType::BOOL:return param.isBool();
                    case VType::INT :return param.isInt();
                    case VType::INT64:return param.isInt64();
                    case VType::UINT:return param.isUInt();
                    case VType::UINT64:return param.isUInt64();
                    case VType::INTERGRAL:return param.isIntegral();
                    case VType::DOUBLE:return param.isDouble();
                    case VType::STRING:return param.isString();
                    case VType::OBJECT:return param.isObject();
                    case VType::ARRAY:return param.isArray();
                }
                return false;
            }
        private:
            std::string _method;
            std::vector<ServiceParam> _service_params;
            VType _return_type;
            ServiceCallBack _service_callback;
        };


        class ServiceDescribeFactory
        {
        public:
            using ptr=std::shared_ptr<ServiceDescribeFactory>;
            ServiceDescribe::ptr Build()
            {
                return std::make_shared<ServiceDescribe>(_method,std::move(_service_params),_return_type,_service_callback);
            }

            ServiceDescribeFactory& SetMethod(const std::string& method)
            {
                _method=method;
                return *this;
            }

            ServiceDescribeFactory& AddParam(const ServiceDescribe::ServiceParam& param)
            {
                _service_params.emplace_back(param);
                return *this;
            }

            ServiceDescribeFactory& SetReturnType(VType return_type)
            {
                _return_type=return_type;
                return *this;
            }
            ServiceDescribeFactory& SetServiceCallBack(const ServiceDescribe::ServiceCallBack& cb)
            {
                _service_callback=cb;
                return *this;
            }

        private:
            std::string _method;
            std::vector<ServiceDescribe::ServiceParam> _service_params;
            VType _return_type;
            ServiceDescribe::ServiceCallBack _service_callback;
        };

        class ServiceManager
        {
        public:
            using ptr=std::shared_ptr<ServiceManager>;
            void Insert(const ServiceDescribe::ptr& service)
            {
                std::unique_lock<std::mutex> lock(_mutex);
                std::string method=service->GetMethod();
                auto ret=_services.insert({method,service});
                if(ret.second)
                {
                    LOG_INFO("RPC服务已注册: method=%s",method.c_str());
                }
                else
                {
                    LOG_WARN("RPC服务重复注册，保留原有处理器: method=%s",method.c_str());
                }
            }
            ServiceDescribe::ptr Select(const std::string& method)
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it=_services.find(method);
                if(it==_services.end())
                {
                    return ServiceDescribe::ptr();
                }
                return it->second;
            }

            void Remove(const std::string& method)
            {
                std::unique_lock<std::mutex> lock(_mutex);
                size_t removed=_services.erase(method);
                if(removed>0)
                {
                    LOG_INFO("RPC服务已移除: method=%s",method.c_str());
                }
            }

        private:
            std::mutex _mutex;
            std::unordered_map<std::string,ServiceDescribe::ptr> _services;
        };


        class RpcRouter
        {
        public:
            using ptr=std::shared_ptr<RpcRouter>;
            RpcRouter():_services(std::make_shared<ServiceManager>()){}
            void OnRpcRequest(const BaseConnection::ptr& conn,const RpcRequest::ptr& request)
            {
                auto service=_services->Select(request->GetMethod());
                if(!service)
                {
                    LOG_WARN("RPC服务不存在: method=%s message_id=%s",request->GetMethod().c_str(),request->GetMessageId().c_str());
                    return Response(conn,request,Json::Value(),ResponseCode::RCODE_NOT_FOUND_SERVICE);
                }
                if(!service->CheckParams(request->GetParams()))
                {
                    return Response(conn,request,Json::Value(),ResponseCode::RCODE_INVALID_PARAMS);
                }
                Json::Value result;
                if(!service->Caller(request->GetParams(),result))
                {
                    LOG_ERROR("RPC服务返回值校验失败: method=%s message_id=%s",request->GetMethod().c_str(),request->GetMessageId().c_str());
                    return Response(conn,request,Json::Value(),ResponseCode::RCODE_INTERNAL_ERROR);
                }
                Response(conn,request,result,ResponseCode::RCODE_OK);
            }
            void RegisterService(const ServiceDescribe::ptr& service)
            {
                _services->Insert(service);
            }
             void RemoveService(const ServiceDescribe::ptr& service)
            {
                _services->Remove(service->GetMethod());
            }
        private:
            void Response(const BaseConnection::ptr& conn,const RpcRequest::ptr& request,const Json::Value& result,const ResponseCode& code)
            {
                auto response=MessageFactory::Create<RpcResponse>();
                response->SetMessageType(MessageType::RESPONSE_RPC);
                response->SetMessageId(request->GetMessageId());
                response->SetResponseCode(code);
                response->SetResponseResult(result);
                conn->Send(response);
            }
        private:
            ServiceManager::ptr _services;
        };
    }
}
