#include"../../source/common/message.hpp"

using namespace JsonRpc;

void TestRpcRequest()
{
    JsonRpc::BaseMessage::ptr ptr=JsonRpc::MessageFactory::Create(JsonRpc::MessageType::REQUEST_RPC);
    JsonRpc::RpcRequest::ptr rpc_ptr=std::dynamic_pointer_cast<JsonRpc::RpcRequest>(ptr);
    if(!rpc_ptr.get())
    {
        LOG_ERROR("RpcRequest创建失败: 返回空指针");
        return;
    }
    rpc_ptr->SetMethod("Add");
    Json::Value params;
    params["num1"]=1;
    params["num2"]=2;
    rpc_ptr->SetParams(params);
    if(!rpc_ptr->Check())
    {
        LOG_ERROR("RpcRequest校验失败: 序列化前");
        return;
    }
    std::string message=rpc_ptr->Serialize();
    LOG_INFO("RpcRequest序列化结果:\n%s",message.c_str());
    RpcRequest::ptr rpc2=std::dynamic_pointer_cast<JsonRpc::RpcRequest>(MessageFactory::Create(MessageType::REQUEST_RPC));
    rpc2->UnSerialize(message);
     if(!rpc2->Check())
    {
        LOG_ERROR("RpcRequest校验失败: 反序列化后");
        return;
    }
}

void TestTopicRequest()
{
    TopicRequest::ptr ptr1=std::dynamic_pointer_cast<TopicRequest>(MessageFactory::Create(JsonRpc::MessageType::REQUEST_TOPIC));
    if(!ptr1.get())
    {
        LOG_ERROR("TopicRequest创建失败: 返回空指针");
        return;
    }
    ptr1->SetTopicKey("hello");
    ptr1->SetTopicOptype(TopicOptype::TOPIC_PUBLISH);
    ptr1->SetTopicMsg("Hello World");
    if(!ptr1->Check())
    {
        LOG_ERROR("TopicRequest校验失败: 序列化前");
        return;
    }
    std::string message=ptr1->Serialize();
    LOG_INFO("TopicRequest序列化结果:\n%s",message.c_str());
    TopicRequest::ptr rpc2=std::dynamic_pointer_cast<JsonRpc::TopicRequest>(MessageFactory::Create(MessageType::REQUEST_TOPIC));
    rpc2->UnSerialize(message);
     if(!rpc2->Check())
    {
        LOG_ERROR("TopicRequest校验失败: 反序列化后");
        return;
    }
}

void TestServiceRequest()
{
    ServiceRequest::ptr ptr1=std::dynamic_pointer_cast<ServiceRequest>(MessageFactory::Create(JsonRpc::MessageType::REQUEST_SERVICE));
    if(!ptr1.get())
    {
        LOG_ERROR("ServiceRequest创建失败: 返回空指针");
        return;
    }
    ptr1->SetMethod("Add");
    ptr1->SetServiceOptype(ServiceOptype::SERVICE_REGISTER);
    ptr1->SetHost({"127.0.0.1",8080});
    ptr1->SetMessageId(Util::UUID());
   
    if(!ptr1->Check())
    {
        LOG_ERROR("ServiceRequest校验失败: 序列化前");
        return;
    }
    std::string message=ptr1->Serialize();
    LOG_INFO("ServiceRequest序列化结果:\n%s",message.c_str());
    ServiceRequest::ptr rpc2=std::dynamic_pointer_cast<JsonRpc::ServiceRequest>(MessageFactory::Create(MessageType::REQUEST_SERVICE));
    rpc2->UnSerialize(message);
     if(!rpc2->Check())
    {
        LOG_ERROR("ServiceRequest校验失败: 反序列化后");
        return;
    }
}

void TestRpcResponse()
{
    RpcResponse::ptr ptr1=std::dynamic_pointer_cast<RpcResponse>(MessageFactory::Create(JsonRpc::MessageType::RESPONSE_RPC));
    if(!ptr1.get())
    {
        LOG_ERROR("RpcResponse创建失败: 返回空指针");
        return;
    }
    ptr1->SetResponseCode(ResponseCode::RCODE_DISCONNECT);
    Json::Value value;
    value["result"]=1;
    ptr1->SetResponseResult(value);
    ptr1->SetMessageId(Util::UUID());
   
    if(!ptr1->Check())
    {
        LOG_ERROR("RpcResponse校验失败: 序列化前");
        return;
    }
    std::string message=ptr1->Serialize();
    LOG_INFO("RpcResponse序列化结果:\n%s",message.c_str());
    RpcResponse::ptr rpc2=std::dynamic_pointer_cast<JsonRpc::RpcResponse>(MessageFactory::Create(MessageType::RESPONSE_RPC));
    rpc2->UnSerialize(message);
     if(!rpc2->Check())
    {
        LOG_ERROR("RpcResponse校验失败: 反序列化后");
        return;
    }
}

void TestTopicResponse()
{
    TopicResponse::ptr ptr1=std::dynamic_pointer_cast<TopicResponse>(MessageFactory::Create(JsonRpc::MessageType::RESPONSE_TOPIC));
    if(!ptr1.get())
    {
        LOG_ERROR("TopicResponse创建失败: 返回空指针");
        return;
    }
    ptr1->SetResponseCode(ResponseCode::RCODE_OK);
    
   
    if(!ptr1->Check())
    {
        LOG_ERROR("TopicResponse校验失败: 序列化前");
        return;
    }
    std::string message=ptr1->Serialize();
    LOG_INFO("TopicResponse序列化结果:\n%s",message.c_str());
    TopicResponse::ptr rpc2=std::dynamic_pointer_cast<JsonRpc::TopicResponse>(MessageFactory::Create(MessageType::RESPONSE_TOPIC));
    rpc2->UnSerialize(message);
     if(!rpc2->Check())
    {
        LOG_ERROR("TopicResponse校验失败: 反序列化后");
        return;
    }
}

void TestServiceResponse()
{
    ServiceResponse::ptr ptr1=std::dynamic_pointer_cast<ServiceResponse>(MessageFactory::Create(JsonRpc::MessageType::RESPONSE_SERVICE));
    if(!ptr1.get())
    {
        LOG_ERROR("ServiceResponse创建失败: 返回空指针");
        return;
    }
    ptr1->SetResponseCode(ResponseCode::RCODE_OK);
    ptr1->SetServiceOptype(ServiceOptype::SERVICE_DISCOVER);
    ptr1->SetMethod("Add");
    std::vector<Address> hosts;
    hosts.emplace_back("127.0.0.1",8080);
    hosts.emplace_back("127.0.0.1",9090);
    hosts.emplace_back("127.0.0.1",10000);
    ptr1->SetHosts(hosts);
   
    if(!ptr1->Check())
    {
        LOG_ERROR("ServiceResponse校验失败: 序列化前");
        return;
    }
    std::string message=ptr1->Serialize();
    LOG_INFO("ServiceResponse序列化结果:\n%s",message.c_str());
    ServiceResponse::ptr rpc2=std::dynamic_pointer_cast<ServiceResponse>(MessageFactory::Create(MessageType::RESPONSE_SERVICE));
    rpc2->UnSerialize(message);
    if(!rpc2->Check())
    {
        LOG_ERROR("ServiceResponse校验失败: 反序列化后");
        return;
    }
    LOG_INFO("ServiceResponse解析结果: rcode=%d optype=%d method=%s",(int)rpc2->GetResponseCode(),(int)rpc2->GetServiceOptype(),rpc2->GetMethod().c_str());
    auto addresses=rpc2->GetHosts();
    for(auto& host:addresses)
    {
        LOG_INFO("发现服务节点: ip=%s port=%d",host.first.c_str(),host.second);
    }
}


int main()
{
    TestRpcRequest();
    TestTopicRequest();
    TestServiceRequest();
    TestRpcResponse();
    TestTopicResponse();
    TestServiceResponse();
    return 0;
}
