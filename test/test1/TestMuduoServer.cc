#include"../source/common/net.hpp"
#include"../source/common/message.hpp"
#include"../source/common/dispatcher.hpp"
#include"../source/server/rpc_router.hpp"
using namespace JsonRpc;

int main()
{

    auto router=std::make_shared<Server::RpcRouter>();
    auto service_fatory=std::make_shared<Server::ServiceDescribeFactory>();
    service_fatory->SetMethod("Add").AddParam({"num1",Server::VType::INT}).AddParam({"num2",Server::VType::INT}).SetReturnType(Server::VType::INT).SetServiceCallBack([](const Json::Value& params,Json::Value& result){
        int num1=params["num1"].asInt();
        int num2=params["num2"].asInt();
        result=num1+num2;
    });
    auto service=service_fatory->Build();
    router->RegisterService(service);

    auto OnRpcRequest=std::bind(&Server::RpcRouter::OnRpcRequest,router.get(),std::placeholders::_1,std::placeholders::_2);

    auto dispatcher=std::make_shared<Dispatcher>();

    auto messagecallback=std::bind(&Dispatcher::OnMessage,dispatcher.get(),std::placeholders::_1,std::placeholders::_2);

    dispatcher->RegisterHandler<RpcRequest>(MessageType::REQUEST_RPC,OnRpcRequest);

    auto server=ServerFactory::Create("127.0.0.1",8080);
    server->SetMessageCallBack(messagecallback);
    server->SetConnectCallBack([](const BaseConnection::ptr& conn){
        LOG_INFO("link a connection");
    });
    server->SetCloseCallBack([](const BaseConnection::ptr& conn){
        LOG_INFO("close a connection");
    });
    server->Start();
    return 0;
}