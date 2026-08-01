#pragma once
#include"../common/net.hpp"
#include<unordered_set>
namespace JsonRpc
{
    namespace Server
    {
        class Provider
        {
        public:
            using ptr=std::shared_ptr<Provider>;
            Provider(const BaseConnection::ptr& connection,const Address& addr):_conn(connection),_host(addr)
            {}
            void AppendMethod(const std::string& method)
            {
                std::unique_lock<std::mutex> lock(_mutex);
                _methods.emplace_back(method);
            }
            std::vector<std::string> GetMethods()const
            {
                std::unique_lock<std::mutex> lock(_mutex);
                return _methods;
            }
            Address GetHost()const
            {
                return _host;
            }
        private:
            mutable std::mutex _mutex;
            BaseConnection::ptr _conn;
            std::vector<std::string> _methods;
            Address _host;
        };

       class ProviderManager
       {
        public:
            using ptr=std::shared_ptr<ProviderManager>;
          
            void AddProvider(const BaseConnection::ptr& conn,const Address& host,const std::string& method)
            {
                Provider::ptr provider;
                {
                    std::unique_lock<std::mutex> lock(_mutex);
                    auto it=_conns.find(conn);
                    if(it==_conns.end())
                    {
                        provider=std::make_shared<Provider>(conn,host);
                        if(!provider)
                        {
                            LOG_ERROR("构造provider失败");
                            return;
                        }
                        _conns.insert({conn,provider});
                    }
                    else
                    {
                        provider=it->second;
                    }

                    _providers[method].insert(provider);
                }

                provider->AppendMethod(method); 
            }

            Provider::ptr GetProvider(const BaseConnection::ptr& conn)const
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it=_conns.find(conn);
                if(it==_conns.end())
                {
                    return Provider::ptr();
                }
                return it->second;
            }

            void RemoveProvider(const BaseConnection::ptr& conn)
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it=_conns.find(conn);
                if(it==_conns.end())
                {
                    return;
                }
                for(auto& method:it->second->GetMethods())
                {
                    auto& providers=_providers[method];
                    providers.erase(it->second);
                }
                _conns.erase(it);
            }

            std::vector<Address> GetHosts(const std::string& method)const
            {
                
                std::unique_lock<std::mutex> lock(_mutex);
                auto it=_providers.find(method);
                if(it==_providers.end())
                {
                    return std::vector<Address>();
                }
                auto providers=&(it->second);
                
                std::vector<Address> hosts;
                for(auto it=providers->begin();it!=providers->end();++it)
                {
                    hosts.emplace_back((*it)->GetHost());
                }
                return hosts;
            }

        private:
            mutable std::mutex _mutex;
            std::unordered_map<std::string,std::unordered_set<Provider::ptr>> _providers;
            std::unordered_map<BaseConnection::ptr,Provider::ptr> _conns;
       };

        class Discoverer
        {
        public:
            using ptr=std::shared_ptr<Discoverer>;
           
            Discoverer(const BaseConnection::ptr& connection):_conn(connection)
            {}
            void AppendMethod(const std::string& method)
            {
                std::unique_lock<std::mutex> lock(_mutex);
                _methods.emplace_back(method);
            }
            std::vector<std::string> GetMethods()const
            {
                std::unique_lock<std::mutex> lock(_mutex);
                return _methods;
            }
            BaseConnection::ptr GetConnection()
            {
                return _conn;
            }
        private:
            mutable std::mutex _mutex;
            BaseConnection::ptr _conn;
            std::vector<std::string> _methods;
        };

       class DiscovererManager
       {
        public:
            using ptr=std::shared_ptr<DiscovererManager>;
           

            void AddDiscoverer(const BaseConnection::ptr& conn,const std::string method)
            {
                Discoverer::ptr discoverer;
                {
                    std::unique_lock<std::mutex> lock(_mutex);
                    auto it=_conns.find(conn);
                    if(it==_conns.end())
                    {
                        discoverer=std::make_shared<Discoverer>(conn);
                        if(!discoverer)
                        {
                            LOG_ERROR("构造discoverer失败");
                            return;
                        }
                        _conns.insert({conn,discoverer});
                    }
                    else
                    {
                        discoverer=it->second;
                    }

                    _discoverers[method].insert(discoverer);
                }

                discoverer->AppendMethod(method); 
            }

            void RemoveDiscoverer(const BaseConnection::ptr& conn)
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it=_conns.find(conn);
                if(it==_conns.end())
                {
                    return;
                }
                for(auto& method:it->second->GetMethods())
                {
                    auto& discoverers=_discoverers[method];
                    discoverers.erase(it->second);
                }
                _conns.erase(it);
            }

            void OnlineNotify(const std::string& method,const Address& host)
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it=_discoverers.find(method);
                if(it==_discoverers.end())
                {
                    return;
                }
                auto message=MessageFactory::Create<ServiceRequest>();
                message->SetMessageType(MessageType::REQUEST_SERVICE);
                message->SetMessageId(Util::UUID());
                message->SetMethod(method);
                message->SetHost(host);
                message->SetServiceOptype(ServiceOptype::SERVICE_ONLINE);

                for(auto& discoverer:it->second)
                {
                    discoverer->GetConnection()->Send(message);
                }
            }

            void OfflineNotify(const std::string& method,const Address& host)
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it=_discoverers.find(method);
                if(it==_discoverers.end())
                {
                    return;
                }
                auto message=MessageFactory::Create<ServiceRequest>();
                message->SetMessageType(MessageType::REQUEST_SERVICE);
                message->SetMessageId(Util::UUID());
                message->SetMethod(method);
                message->SetHost(host);
                message->SetServiceOptype(ServiceOptype::SERVICE_OFFLINE);

                for(auto& discoverer:it->second)
                {
                    discoverer->GetConnection()->Send(message);
                }
            }

        private:
            std::mutex _mutex;
            std::unordered_map<std::string,std::unordered_set<Discoverer::ptr>> _discoverers;
            std::unordered_map<BaseConnection::ptr,Discoverer::ptr> _conns;
       };


       class ProviderDiscovererManager
       {
        public:
            using ptr=std::shared_ptr<ProviderDiscovererManager>;
            ProviderDiscovererManager():_providers(std::make_shared<ProviderManager>()),_discoverers(std::make_shared<DiscovererManager>())
            {}

            void OnServiceRequest(const BaseConnection::ptr& conn,const ServiceRequest::ptr& request)
            {
                if(request->GetServiceOptype()==ServiceOptype::SERVICE_REGISTER)
                {
                    std::string method=request->GetMethod();
                    Address host=request->GetHost();
                    _providers->AddProvider(conn,host,method);
                    _discoverers->OnlineNotify(method,host);
                    RegisterResponse(conn,request);
                }
                else if(request->GetServiceOptype()==ServiceOptype::SERVICE_DISCOVER)
                {
                    std::string method=request->GetMethod();
                    _discoverers->AddDiscoverer(conn,method);
                    DiscoverResponse(conn,request);
                }
                else
                {
                    LOG_ERROR("服务类型错误");
                    ErrorResponse(conn,request);
                }
            }

            void OnConnectionShutdown(const BaseConnection::ptr& conn)
            {
                auto provider=_providers->GetProvider(conn);
                if(provider)
                {
                    for(auto& method:provider->GetMethods())
                    {
                        _discoverers->OfflineNotify(method,provider->GetHost());
                    }
                    _providers->RemoveProvider(conn);
                }
                _discoverers->RemoveDiscoverer(conn);
            }
        private:
            void RegisterResponse(const BaseConnection::ptr& conn,const ServiceRequest::ptr& request)
            {
                auto response=MessageFactory::Create<ServiceResponse>();
                response->SetMessageType(MessageType::RESPONSE_SERVICE);
                response->SetMessageId(request->GetMessageId());
                response->SetResponseCode(ResponseCode::RCODE_OK);
                response->SetServiceOptype(ServiceOptype::SERVICE_REGISTER);
                conn->Send(response);
            }

            void ErrorResponse(const BaseConnection::ptr& conn,const ServiceRequest::ptr& request)
            {
                auto response=MessageFactory::Create<ServiceResponse>();
                response->SetMessageType(MessageType::RESPONSE_SERVICE);
                response->SetMessageId(request->GetMessageId());
                response->SetServiceOptype(request->GetServiceOptype());
                response->SetResponseCode(ResponseCode::RCODE_INVALID_OPTYPE);
                conn->Send(response);
            }

            void DiscoverResponse(const BaseConnection::ptr& conn,const ServiceRequest::ptr& request)
            {
                auto response=MessageFactory::Create<ServiceResponse>();
                response->SetMessageType(MessageType::RESPONSE_SERVICE);
                response->SetMessageId(request->GetMessageId());
                response->SetMethod(request->GetMethod());
                response->SetServiceOptype(ServiceOptype::SERVICE_DISCOVER);
                std::vector<Address> hosts=_providers->GetHosts(request->GetMethod());
                if(hosts.empty())
                {
                    response->SetResponseCode(ResponseCode::RCODE_NOT_FOUND_SERVICE);
                }
                else
                {
                    response->SetResponseCode(ResponseCode::RCODE_OK);
                }
                response->SetHosts(hosts);
                conn->Send(response);
            }


        private:
            ProviderManager::ptr _providers;
            DiscovererManager::ptr _discoverers;
       };
    }
}