#include"../../source/server/rpc_server.hpp"
using namespace JsonRpc;


int main()
{
    Server::RegisterServer server("0.0.0.0",8080);
    server.Start();
    return 0;
}