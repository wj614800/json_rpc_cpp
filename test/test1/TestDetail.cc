#include"../../source/common/detail.hpp"
#include"../../source/common/fields.hpp"
void TestLog()
{
    LOG_INFO("日志基础输出测试");
    LOG_INFO("日志单参数格式化测试: 1 + 1 = %d",2);
    LOG_INFO("日志多参数格式化测试: %d + %d = %d",100,200,300);
    LOG_INFO("日志字符串格式化测试: message=%s","Hello World");
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
    LOG_INFO("JSON序列化结果:\n%s",content.c_str());
    Json::Value other;
    JsonRpc::Util::UnSerialize(content,other);
    std::cout<<other<<std::endl;
}

void TestUUID()
{
    for(int i=1;i<=10;i++)
    {
        LOG_INFO("生成UUID: value=%s",JsonRpc::Util::UUID().c_str());
    }
}

void TestErrorReason()
{
    LOG_INFO("响应码说明: code=RCODE_OK reason=%s",JsonRpc::Util::ErrorReason(JsonRpc::ResponseCode::RCODE_OK).c_str());
    LOG_INFO("响应码说明: code=RCODE_INVALID_MSG reason=%s",JsonRpc::Util::ErrorReason(JsonRpc::ResponseCode::RCODE_INVALID_MSG).c_str());
}

int main()
{
    TestLog();
    TestJsonUtil();
    TestUUID();
    TestErrorReason();
    return 0;
}
