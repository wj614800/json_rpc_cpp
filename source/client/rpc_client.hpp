#pragma once
#include"../common/dispatcher.hpp"
#include"../common/net.hpp"
#include"rpc_registry.hpp"
#include"rpc_caller.hpp"
#include"requestor.hpp"
#include"rpc_topic.hpp"


namespace JsonRpc
{
    namespace Client
    {
        class DiscoverClient
        {
        public:
            using ptr=std::shared_ptr<DiscoverClient>;
            DiscoverClient(const std::string& ip,uint16_t port,const Discoverer::OfflineCallBack& cb)
                :_requestor(std::make_shared<Requestor>())
                ,_discover(std::make_shared<Discoverer>(_requestor,cb))
                ,_dispatcher(std::make_shared<Dispatcher>())
            {
                auto onServiceRequest=std::bind(&Discoverer::OnServiceRequest,_discover.get(),std::placeholders::_1,std::placeholders::_2);
                _dispatcher->RegisterHandler<ServiceRequest>(MessageType::REQUEST_SERVICE,onServiceRequest);

                auto  onServiceResponse=std::bind(&Requestor::OnResponse,_requestor.get(),std::placeholders::_1,std::placeholders::_2);
                _dispatcher->RegisterHandler<ServiceResponse>(MessageType::RESPONSE_SERVICE,onServiceResponse);

                auto onMessage=std::bind(&Dispatcher::OnMessage,_dispatcher.get(),std::placeholders::_1,std::placeholders::_2);

                _client=ClientFactory::Create(ip,port);
                _client->SetMessageCallBack(onMessage);
                _client->Connect();
            }

            bool ServiceDiscover(const std::string& method,Address& host)
            {
                return _discover->ServiceDiscover(_client->Connection(),method,host);
            }

        private:
            Requestor::ptr _requestor;
            Discoverer::ptr _discover;
            Dispatcher::ptr _dispatcher;
            BaseClient::ptr _client;
        };


        class ProviderClient
        {
        public:
            using ptr=std::shared_ptr<ProviderClient>;
            ProviderClient(const std::string& ip,uint16_t port)
                :_requestor(std::make_shared<Requestor>())
                ,_provider(std::make_shared<Provider>(_requestor))
                ,_dispatcher(std::make_shared<Dispatcher>())
            {
                auto onResponse=std::bind(&Requestor::OnResponse,_requestor.get(),std::placeholders::_1,std::placeholders::_2);
                _dispatcher->RegisterHandler<ServiceResponse>(MessageType::RESPONSE_SERVICE,onResponse);

                auto onMessage=std::bind(&Dispatcher::OnMessage,_dispatcher.get(),std::placeholders::_1,std::placeholders::_2);

                _client=ClientFactory::Create(ip,port);
                _client->SetMessageCallBack(onMessage);
                _client->Connect();
            }
            bool RegisterMethod(const std::string& method,const Address& host)
            {
                return _provider->RegisterMethod(_client->Connection(),method,host);
            }
        private:
            Requestor::ptr _requestor;
            Provider::ptr _provider;
            Dispatcher::ptr _dispatcher;
            BaseClient::ptr _client;
        };


        class RpcClient
        {
        public:
            using ptr=std::shared_ptr<RpcClient>;
            RpcClient(const std::string& ip,uint16_t port,bool enable_register_server=true)
                :_enable_register_server(enable_register_server)
                ,_requestor(std::make_shared<Requestor>())
                ,_rpc_caller(std::make_shared<RpcCaller>(_requestor))
                ,_dispatcher(std::make_shared<Dispatcher>())
            {
                auto onResponse=std::bind(&Requestor::OnResponse,_requestor.get(),std::placeholders::_1,std::placeholders::_2);
                _dispatcher->RegisterHandler<RpcResponse>(MessageType::RESPONSE_RPC,onResponse);
                if(_enable_register_server)
                {
                    _discover_client=std::make_shared<DiscoverClient>(ip,port,std::bind(&RpcClient::RemoveClient,this,std::placeholders::_1));
                }
                else
                {
                  
                    auto onMessage=std::bind(&Dispatcher::OnMessage,_dispatcher.get(),std::placeholders::_1,std::placeholders::_2);
                    _rpc_client=ClientFactory::Create(ip,port);
                    _rpc_client->SetMessageCallBack(onMessage);
                    _rpc_client->Connect();
                }
            }

            bool Call(const std::string& method,const Json::Value& params,Json::Value& response)
            {
                auto rpc_client=GetClient(method);
                if(!rpc_client)
                {
                    return false;
                }
                return _rpc_caller->Call(rpc_client->Connection(),method,params,response);
            }

            bool Call(const std::string& method,const Json::Value& params,RpcCaller::AsyncResponse& response)
            {
                auto rpc_client=GetClient(method);
                if(!rpc_client)
                {
                    return false;
                }
                return _rpc_caller->Call(rpc_client->Connection(),method,params,response);
            }

            bool Call(const std::string& method,const Json::Value& params,const RpcCaller::RequestCallBack& cb)
            {
                auto rpc_client=GetClient(method);
                if(!rpc_client)
                {
                    return false;
                }
                return _rpc_caller->Call(rpc_client->Connection(),method,params,cb);
            }
        private:
            BaseClient::ptr NewClient(const Address& host)
            {
                auto onMessage=std::bind(&Dispatcher::OnMessage,_dispatcher.get(),std::placeholders::_1,std::placeholders::_2);
                auto rpc_client=ClientFactory::Create(host.first,host.second);
                if(!rpc_client)
                {
                    return BaseClient::ptr();
                }
                rpc_client->SetMessageCallBack(onMessage);
                rpc_client->Connect();
                AddClient(host,rpc_client);
                return rpc_client;
            }
            void AddClient(const Address& host,const BaseClient::ptr& client)
            {
                std::unique_lock<std::mutex> lock(_mutex);
                _rpc_clients.insert({host,client});
            }

            BaseClient::ptr GetClient(const Address& host)
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it=_rpc_clients.find(host);
                if(it==_rpc_clients.end())
                {
                    return BaseClient::ptr();
                }
                return it->second;
            }

            BaseClient::ptr GetClient(const std::string& method)
            {
                if(_enable_register_server)
                {
                    Address host;
                    bool ret=_discover_client->ServiceDiscover(method,host);
                    if(ret==false)
                    {
                        return BaseClient::ptr();
                    }
                    auto rpc_client=GetClient(host);
                    if(!rpc_client)
                    {
                        rpc_client=NewClient(host);
                    }
                    return rpc_client;
                }
                else
                {
                    return _rpc_client;
                }
            }

            void RemoveClient(const Address& host)
            {
                std::unique_lock<std::mutex> lock(_mutex);
                _rpc_clients.erase(host);
            }
        private:
            struct AddressHash
            {
                size_t operator()(const Address& host)const
                {
                    std::string hash=host.first+std::to_string(host.second);
                    return std::hash<std::string>{}(hash);
                }
            };


            bool _enable_register_server;//传递的 ip地址和port端口号 是否是注册中心的
            DiscoverClient::ptr _discover_client;
            Requestor::ptr _requestor;
            RpcCaller::ptr _rpc_caller;
            Dispatcher::ptr _dispatcher;
            BaseClient::ptr _rpc_client;
            std::mutex _mutex;
            std::unordered_map<Address,BaseClient::ptr,AddressHash> _rpc_clients;
        };

        class TopicClient
        {
        public:
            using ptr=std::shared_ptr<TopicClient>;
            TopicClient(const std::string& ip,uint16_t port)
                :_requestor(std::make_shared<Requestor>())
                ,_topic_manager(std::make_shared<TopicManager>(_requestor))
                ,_dispatcher(std::make_shared<Dispatcher>())
            {
                auto onResponse=std::bind(&Requestor::OnResponse,_requestor.get(),std::placeholders::_1,std::placeholders::_2);
                auto onPublish=std::bind(&TopicManager::OnPublish,_topic_manager.get(),std::placeholders::_1,std::placeholders::_2);
                _dispatcher->RegisterHandler<TopicRequest>(MessageType::REQUEST_TOPIC,onPublish);
                _dispatcher->RegisterHandler<TopicResponse>(MessageType::RESPONSE_TOPIC,onResponse);
                auto onMessage=std::bind(&Dispatcher::OnMessage,_dispatcher.get(),std::placeholders::_1,std::placeholders::_2);
                _client=ClientFactory::Create(ip,port);
                _client->SetMessageCallBack(onMessage);
                _client->Connect();
            }

            bool CreateTopic(const std::string& topic_key)
            {
                return _topic_manager->Create(_client->Connection(),topic_key);
            }

            bool RemoveTopic(const std::string& topic_key)
            {
                return _topic_manager->Remove(_client->Connection(),topic_key);
            }

            bool CancelTopic(const std::string& topic_key)
            {
                return _topic_manager->Cancel(_client->Connection(),topic_key);
            }

            bool SubscribeTopic(const std::string& topic_key,const TopicManager::PublishCallBack& cb)
            {
                return _topic_manager->Subscribe(_client->Connection(),topic_key,cb);
            }

            bool PublishTopic(const std::string& topic_key,const std::string& topic_msg)
            {
                return _topic_manager->Publish(_client->Connection(),topic_key,topic_msg);
            }
        private:
            Requestor::ptr _requestor;
            TopicManager::ptr _topic_manager;
            Dispatcher::ptr _dispatcher;
            BaseClient::ptr _client;
        };
    }
}
