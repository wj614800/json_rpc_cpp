#include"../source/common/detail.hpp"
#include"../source/common/fields.hpp"
void TestLog()
{
    LOG_INFO("Hello Log");
    LOG_INFO("1 + 1 = %d",2);
    LOG_INFO("%d + %d = %d",100,200,300);
    LOG_INFO("%s","Hello World");
    LOG_WARN("test warning log");
    LOG_ERROR("test error log");
    LOG_FATAL("test fatal log");
}


void TestJsonUtil()
{
    Json::Value root;
    root["name"]="Tom";
    root["age"]=18;
    root["Id"]="2024081001";
    root["score"].append(90);
    root["score"].append(80);
    root["score"].append(100);
    std::string content;
    JsonRpc::Util::Serialize(root,content);
    LOG_INFO("body:\n%s",content.c_str());
    Json::Value other;
    JsonRpc::Util::UnSerialize(content,other);
    std::cout<<other<<std::endl;
}

void TestUUID()
{
    for(int i=1;i<=10;i++)
    {
        LOG_INFO("%s",JsonRpc::Util::UUID().c_str());
    }
}

void TestErrorReason()
{
    LOG_INFO("RCode::RCODE_OK:%s",JsonRpc::Util::ErrorReason(JsonRpc::ResponseCode::RCODE_OK).c_str());
    LOG_INFO("RCode::RCODE_INVALID_MSG:%s",JsonRpc::Util::ErrorReason(JsonRpc::ResponseCode::RCODE_INVALID_MSG).c_str());
}

int main()
{
    TestLog();
    TestJsonUtil();
    TestUUID();
    TestErrorReason();
    return 0;
}