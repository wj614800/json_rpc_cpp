#include"server/rpc_server.hpp"




class Translate
{
public:
    std::string translate(const std::string& word)
    {
        static std::unordered_map<std::string,std::string> words={{"hello","你好"},{"apple","苹果"},{"banana","香蕉"},{"word","单词"}};
        auto it=words.find(word);
        if(it==words.end())
        {
            return "未知";
        }
        return it->second;
    }
};


int main()
{
    Translate translate;
    JsonRpc::Server::RpcServer server("127.0.0.1",8080,true,"127.0.0.1","127.0.0.1",8081);
    JsonRpc::Server::ServiceDescribeFactory factory;
    auto service=factory.SetMethod("translate").SetReturnType(JsonRpc::Server::VType::STRING).AddParam({"str",JsonRpc::Server::VType::STRING}).SetServiceCallBack([&translate](const Json::Value& params,Json::Value& result){
        std::string word=params["str"].asString();
        result=translate.translate(word);
    }).Build();
    server.RegisterService(service);
    server.Start();
    return 0;
}