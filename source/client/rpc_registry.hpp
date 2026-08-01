#pragma once
#include"../common/net.hpp"
#include"requestor.hpp"


namespace JsonRpc
{
    namespace Client
    {
        class Provider
        {
        public:
            using ptr=std::shared_ptr<Provider>;
            Provider(const Requestor::ptr& requestor):_requestor(requestor)
            {}
            bool RegisterMethod(const BaseConnection::ptr& conn,const std::string& method,const Address& host)
            {
                auto request=MessageFactory::Create<ServiceRequest>();
                request->SetMessageType(MessageType::REQUEST_SERVICE);
                request->SetMessageId(Util::UUID());
                request->SetMethod(method);
                request->SetServiceOptype(ServiceOptype::SERVICE_REGISTER);
                request->SetHost(host);
                BaseMessage::ptr response;
                bool ret=_requestor->Send(conn,request,response);
                if(!ret)
                {
                    LOG_ERROR("请求发送失败");
                    return false;
                }
                auto service_response=std::dynamic_pointer_cast<ServiceResponse>(response);
                if(!service_response)
                {
                    LOG_ERROR("请求发送成功，但是响应是空");
                    return false;
                }
                if(service_response->GetResponseCode()!=ResponseCode::RCODE_OK)
                {
                    LOG_ERROR("服务注册失败：%s",Util::ErrorReason(service_response->GetResponseCode()).c_str());
                    return false;
                }
                return true;
            }
        private:
            Requestor::ptr _requestor;
        };

        class MethodHost
        {
        public:
            using ptr=std::shared_ptr<MethodHost>;
            MethodHost():_idx(0)
            {}
            MethodHost(const std::vector<Address>& hosts):_idx(0),_hosts(hosts)
            {}
            Address GetHost()
            {
                std::unique_lock<std::mutex> lock(_mutex);
                if(_hosts.empty())return Address();
                ++_idx;
                return _hosts[_idx%_hosts.size()];
            }
            void AddHost(const Address& host)
            {
                std::unique_lock<std::mutex> lock(_mutex);
                _hosts.emplace_back(host);
            }
            void RemoveHost(const Address& host)
            {
                std::unique_lock<std::mutex> lock(_mutex);
                for(auto it=_hosts.begin();it!=_hosts.end();++it)
                {
                    if(*it==host)
                    {
                        _hosts.erase(it);
                        break;
                    }
                }
            }
            bool Empty()
            {
                std::unique_lock<std::mutex> lock(_mutex);
                return _hosts.empty();
            }
        private:
            std::mutex _mutex;
            uint64_t _idx;
            std::vector<Address> _hosts;
        };

        class Discoverer
        {
        public:
            using OfflineCallBack=std::function<void(const Address&)>;
            using ptr=std::shared_ptr<Discoverer>;
            Discoverer(const  Requestor::ptr& requestor,const OfflineCallBack& callback):_requestor(requestor),_offline_cb(callback)
            {}
            bool ServiceDiscover(const BaseConnection::ptr& conn,const std::string& method,Address& host)
            {
                {
                    std::unique_lock<std::mutex> lock(_mutex);
                    auto it=_method_hosts.find(method);
                    if(it!=_method_hosts.end())
                    {
                       if(!it->second->Empty())
                       {
                           host=it->second->GetHost();
                           return true; 
                       }
                    }
                }
                auto request=MessageFactory::Create<ServiceRequest>();
                request->SetMessageType(MessageType::REQUEST_SERVICE);
                request->SetMessageId(Util::UUID());
                request->SetMethod(method);
                request->SetServiceOptype(ServiceOptype::SERVICE_DISCOVER);
                BaseMessage::ptr response;
                bool ret=_requestor->Send(conn,request,response);
                if(!ret)
                {
                    LOG_ERROR("服务发现请求发送失败");
                    return false;
                }
                auto service_response=std::dynamic_pointer_cast<ServiceResponse>(response);
                if(!service_response)
                {
                    LOG_ERROR("服务发现请求为空");
                    return false;
                }
                if(service_response->GetResponseCode()!=ResponseCode::RCODE_OK)
                {
                    LOG_ERROR("服务发现失败：%s",Util::ErrorReason(service_response->GetResponseCode()).c_str());
                    return false;
                }

                MethodHost::ptr method_host=std::make_shared<MethodHost>(service_response->GetHosts());
                if(method_host->Empty())
                {
                    LOG_WARN("没有服务提供");
                    return false;
                }

                {
                    std::unique_lock<std::mutex> lock(_mutex);
                    _method_hosts[method]=method_host;
                }

                host=method_host->GetHost();
               
                return true;
            }

            void OnServiceRequest(const BaseConnection::ptr& conn,const ServiceRequest::ptr& request)
            {
                ServiceOptype optype=request->GetServiceOptype();
                std::string method=request->GetMethod();
                Address host=request->GetHost();
                if(optype==ServiceOptype::SERVICE_ONLINE)
                {
                    
                    std::unique_lock<std::mutex> lock(_mutex);
                    auto it=_method_hosts.find(method);
                    if(it!=_method_hosts.end())
                    {
                       it->second->AddHost(host);
                    }
                    else
                    {
                        _method_hosts[method]=std::make_shared<MethodHost>();
                        _method_hosts[method]->AddHost(host);
                    }
                    
                }
                else if(optype==ServiceOptype::SERVICE_OFFLINE)
                {
                    std::unique_lock<std::mutex> lock(_mutex);
                    auto it=_method_hosts.find(method);
                    if(it!=_method_hosts.end())
                    {
                       it->second->RemoveHost(host);
                    }
                    if(_offline_cb)_offline_cb(host);
                }
                else
                {
                    LOG_ERROR("服务请求类型错误");
                }
            }

        private:
            std::mutex _mutex;
            std::unordered_map<std::string,MethodHost::ptr> _method_hosts;
            Requestor::ptr _requestor;
            OfflineCallBack _offline_cb;
        };
    }
}