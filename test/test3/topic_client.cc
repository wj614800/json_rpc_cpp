#include"../../source/client/rpc_client.hpp"


int main()
{
    JsonRpc::Client::TopicClient client("127.0.0.1",8080);
    bool ret=client.CreateTopic("hello");
    if(!ret)
    {
        LOG_INFO("创建主题失败");
        exit(1);
    }
    uint64_t cnt=0;
    while(1)
    {
        client.PublishTopic("hello","hello world"+std::to_string(cnt));
        ++cnt;
    }
    return 0;
}
