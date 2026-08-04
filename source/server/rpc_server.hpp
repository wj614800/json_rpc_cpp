#pragma once
#include"../common/net.hpp"
#include"../common/dispatcher.hpp"
#include"../client/rpc_client.hpp"
#include"rpc_router.hpp"
#include"rpc_registry.hpp"
#include"rpc_topic.hpp"
namespace JsonRpc
{
    namespace Server
    {
        class RegisterServer
        {
        public:
            using ptr=std::shared_ptr<RegisterServer>;
            RegisterServer(const std::string& ip,uint16_t port)
                :_pdmanager(std::make_shared<ProviderDiscovererManager>())
                ,_dispatcher(std::make_shared<Dispatcher>())
            {
                auto onServiceRequest=std::bind(&ProviderDiscovererManager::OnServiceRequest,_pdmanager.get(),std::placeholders::_1,std::placeholders::_2);
                _dispatcher->RegisterHandler<ServiceRequest>(MessageType::REQUEST_SERVICE,onServiceRequest);
                auto onMessage=std::bind(&Dispatcher::OnMessage,_dispatcher.get(),std::placeholders::_1,std::placeholders::_2);
                _server=ServerFactory::Create(ip,port);
                _server->SetMessageCallBack(onMessage);
                _server->SetCloseCallBack(std::bind(&ProviderDiscovererManager::OnConnectionShutdown,_pdmanager.get(),std::placeholders::_1));
            }

            void Start()
            {
                _server->Start();
            }
        private:
            ProviderDiscovererManager::ptr _pdmanager;
            Dispatcher::ptr _dispatcher;
            BaseServer::ptr _server;
        };

        class RpcServer
        {
        public:
            using ptr=std::shared_ptr<RpcServer>;
            RpcServer(const std::string& bind_ip,uint16_t port,bool enable_register_server=false,const std::string& access_ip="",const std::string& register_ip="",uint16_t register_port=0)
                :_enable_register_server(enable_register_server)
                ,_host(access_ip,port)
                ,_dispatcher(std::make_shared<Dispatcher>())
                ,_rpc_router(std::make_shared<RpcRouter>())
            {
                if(_enable_register_server)
                {
                    _provider_client=std::make_shared<Client::ProviderClient>(register_ip,register_port);
                }
                auto onRpcRequest=std::bind(&RpcRouter::OnRpcRequest,_rpc_router.get(),std::placeholders::_1,std::placeholders::_2);
                _dispatcher->RegisterHandler<RpcRequest>(MessageType::REQUEST_RPC,onRpcRequest);
                auto onMessage=std::bind(&Dispatcher::OnMessage,_dispatcher.get(),std::placeholders::_1,std::placeholders::_2);
                _server=ServerFactory::Create(bind_ip,port);
                _server->SetMessageCallBack(onMessage);
            }

            void RegisterService(const ServiceDescribe::ptr& service)
            {
                if(_enable_register_server)
                {
                    if(!_provider_client->RegisterMethod(service->GetMethod(),_host))
                    {
                        LOG_ERROR("向注册中心注册RPC服务失败: method=%s host=%s:%d",service->GetMethod().c_str(),_host.first.c_str(),_host.second);
                    }
                }
                _rpc_router->RegisterService(service);
            }

            void Start()
            {
                _server->Start();
            }
        private:
            bool _enable_register_server;
            Address _host;
            Client::ProviderClient::ptr _provider_client;
            Dispatcher::ptr _dispatcher;
            RpcRouter::ptr _rpc_router;
            BaseServer::ptr _server;
        };

        class TopicServer
        {
        public:
            using ptr=std::shared_ptr<TopicServer>;
            TopicServer(const std::string& ip,uint16_t port)
                :_dispatcher(std::make_shared<Dispatcher>())
                ,_topic_manager(std::make_shared<TopicManager>())
                {
                     auto onTopicRequest=std::bind(&TopicManager::OnTopicRequest,_topic_manager.get(),std::placeholders::_1,std::placeholders::_2);
                    _dispatcher->RegisterHandler<TopicRequest>(MessageType::REQUEST_TOPIC,onTopicRequest);
                    auto onMessage=std::bind(&Dispatcher::OnMessage,_dispatcher.get(),std::placeholders::_1,std::placeholders::_2);
                    _server=ServerFactory::Create(ip,port);
                    _server->SetMessageCallBack(onMessage);
                    _server->SetCloseCallBack(std::bind(&TopicManager::OnCloseCallBack,_topic_manager.get(),std::placeholders::_1));
                }
            void Start()
            {
                _server->Start();
            }
        private:
            Dispatcher::ptr _dispatcher;
            TopicManager::ptr _topic_manager;
            BaseServer::ptr _server;
        };
    }
}
