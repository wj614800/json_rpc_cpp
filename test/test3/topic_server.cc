#include"../../source/server/rpc_server.hpp"


int main()
{
    JsonRpc::Server::TopicServer server("0.0.0.0",8080);
    server.Start();
    return 0;
}