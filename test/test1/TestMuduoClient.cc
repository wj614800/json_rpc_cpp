#include"../source/common/net.hpp"
#include"../source/common/abstract.hpp"
#include"../source/common/dispatcher.hpp"
#include"../source/client/requestor.hpp"
#include"../source/client/rpc_caller.hpp"
using namespace JsonRpc;

int main()
{

    auto requestor=std::make_shared<Client::Requestor>();
    auto caller=std::make_shared<Client::RpcCaller>(requestor);    

    auto OnResponse=std::bind(&Client::Requestor::OnResponse,requestor.get(),std::placeholders::_1,std::placeholders::_2);

    auto dispatcher=std::make_shared<Dispatcher>();
    auto messagecallback=std::bind(&Dispatcher::OnMessage,dispatcher.get(),std::placeholders::_1,std::placeholders::_2);

    dispatcher->RegisterHandler<RpcResponse>(MessageType::RESPONSE_RPC,OnResponse);

    auto  client=ClientFactory::Create("127.0.0.1",8080);
    client->SetMessageCallBack(messagecallback);
    client->Connect();
    auto conn=client->Connection();

    Json::Value params,result;
    params["num1"]="90";
    params["num2"]=100;
    
   
    bool ret=caller->Call(conn,"Add",params,result);
    if(ret)
    {
        LOG_INFO("result=%d",result.asInt());
    }

    params["num1"]=1000;
    params["num2"]=100;
    std::future<Json::Value> response;
    ret=caller->Call(conn,"Add",params,response);
    if(ret)
    {
        LOG_INFO("do other thing");
        result=response.get();
        if(!result.empty())
        LOG_INFO("result=%d",result.asInt());
    }
    params["num1"]=95;
    params["num2"]=20;
    caller->Call(conn,"Add",params,[](const BaseMessage::ptr& message){
        LOG_INFO("receive a result");
    });
    caller->Call(conn,"Sub",params,result);
    std::this_thread::sleep_for(std::chrono::seconds(3));
    
    

    return 0;
}