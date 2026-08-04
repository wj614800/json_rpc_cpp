#include"client/rpc_client.hpp"
int main()
{
    JsonRpc::Client::RpcClient client("127.0.0.1",8081,true);
    Json::Value params,result;
    while(true)
    {
        std::string word;
        std::getline(std::cin,word);
        params["str"]=word;
        bool ret=client.Call("translate",params,result);
        if(ret)
        {
            LOG_INFO("翻译调用成功: input=%s result=%s",word.c_str(),result.asCString());
        }
        else
        {
            LOG_WARN("调用失败");
        }
    }
    return 0;
}
