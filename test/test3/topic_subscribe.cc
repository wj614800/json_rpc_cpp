#include"../../source/client/rpc_client.hpp"


int main()
{
    JsonRpc::Client::TopicClient client("127.0.0.1",8080);
    bool ret=client.CreateTopic("hello");
    if(!ret)
    {
        LOG_ERROR("创建主题失败: topic=hello");
    }
    client.SubscribeTopic("hello",[](const std::string& key,const std::string& msg){
        LOG_INFO("收到主题消息: topic=%s message=%s",key.c_str(),msg.c_str());
    });
    std::this_thread::sleep_for(std::chrono::minutes(60));
    return 0;
}
