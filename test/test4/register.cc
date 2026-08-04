#include"server/rpc_server.hpp"
int main()
{
    JsonRpc::Server::RegisterServer server("127.0.0.1",8081);
    server.Start();
    return 0;
}