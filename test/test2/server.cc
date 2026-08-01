#include"../../source/server/rpc_server.hpp"

using namespace JsonRpc;

int main()
{
    Server::RpcServer server("0.0.0.0",8081,true,"127.0.0.1","127.0.0.1",8080);
    Server::ServiceDescribeFactory::ptr service_fatory=std::make_shared<Server::ServiceDescribeFactory>();
    service_fatory->SetMethod("Add").AddParam({"num1",Server::VType::INT}).AddParam({"num2",Server::VType::INT}).SetReturnType(Server::VType::INT).SetServiceCallBack([](const Json::Value& params,Json::Value& result){
        int num1=params["num1"].asInt();
        int num2=params["num2"].asInt();
        result=num1+num2;
    });
    server.RegisterService(service_fatory->Build());
    server.Start();
}