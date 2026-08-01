#include"../../source/client/rpc_client.hpp"

using namespace JsonRpc;

int main()
{
    Client::RpcClient client("127.0.0.1",8080,true);
    Json::Value params,result;
    params["num1"]=100;
    params["num2"]=110;
    for(int i=0;i<100000;i++)
    {
        params["num2"]=i;
        bool ret=client.Call("Add",params,result);
        if(ret)
        {
            LOG_INFO("result:%d",result.asInt());
        }
    }
}