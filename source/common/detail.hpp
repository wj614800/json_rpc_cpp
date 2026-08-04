/*
    一些项目中琐碎的函数
    1. 日志宏，方便打印和调试
    2. 序列化和反序列化的函数
    3. UUID的生成函数
    4. 响应状态码转化为字符串的函数
*/
#pragma once
#include<iostream>
#include<cstdio>
#include<ctime>
#include<string>
#include<memory>
#include<sstream>
#include<random>
#include<iomanip>
#include<atomic>
#include<unordered_map>
#include<jsoncpp/json/json.h>
#include"fields.hpp"


enum LogLevel
{
    INFO,
    WARN,
    ERROR,
    FATAL,
    NoLog
};

#define LOGLEVEL INFO

#define LOG(level,format,...) do{\
    if(level<LOGLEVEL)break;\
    time_t current_time=time(0);\
    struct tm tm_struct;\
    localtime_r(&current_time,&tm_struct);\
    char log_time_buffer[64];\
    strftime(log_time_buffer,sizeof(log_time_buffer),"%Y-%m-%d %H:%M:%S",&tm_struct);\
    fprintf(stdout,"[%s][%s][%s:%d] " format "\n",#level,log_time_buffer,__FILE__,__LINE__,##__VA_ARGS__);\
}while(0)

#define LOG_INFO(format,...) LOG(INFO,format,##__VA_ARGS__)
#define LOG_WARN(format,...) LOG(WARN,format,##__VA_ARGS__)
#define LOG_ERROR(format,...) LOG(ERROR,format,##__VA_ARGS__)
#define LOG_FATAL(format,...) LOG(FATAL,format,##__VA_ARGS__)

namespace JsonRpc
{
    class Util
    {
    public:
        static bool Serialize(const Json::Value& root,std::string& body)
        {
            Json::StreamWriterBuilder swb;
            std::unique_ptr<Json::StreamWriter> sw(swb.newStreamWriter());
            std::stringstream ss;
            if(sw->write(root,&ss)!=0)
            {
                LOG_ERROR("JSON序列化失败");
                return false;
            }
            body=ss.str();
            return true;
        }

        static bool UnSerialize(const std::string& body,Json::Value& root)
        {
            Json::CharReaderBuilder crb;
            std::unique_ptr<Json::CharReader> cr(crb.newCharReader());
            std::string err;
            if(!cr->parse(body.c_str(),body.c_str()+body.size(),&root,&err))
            {
                LOG_WARN("JSON反序列化失败: reason=%s",err.c_str());
                return false;
            }
            return true;
        }

        static std::string UUID()
        {
            std::random_device rd;
            std::mt19937 generator(rd());
            std::uniform_int_distribution dis(0,255);
            std::stringstream ss;
            for(int i=0;i<8;i++)
            {
                if(i==4||i==6)ss<<"-";
                ss<<std::setfill('0')<<std::setw(2)<<std::hex<<dis(generator);
            }
            ss<<"-";
            static std::atomic<uint64_t> count(1);
            uint64_t value=count.fetch_add(1);
            for(int i=7;i>=0;i--)
            {
                if(i==5)ss<<"-";
                ss<<std::setfill('0')<<std::setw(2)<<std::hex<<(value>>(i*8)&0xff);
            } 
            return ss.str();
        }

        static std::string ErrorReason(const ResponseCode& code)
        {
            static std::unordered_map<ResponseCode,std::string> err_map={
                {ResponseCode::RCODE_OK,"success"},
                {ResponseCode::RCODE_PARSE_FAILED,"解析失败"},
                {ResponseCode::RCODE_DISCONNECT,"连接断开"},
                {ResponseCode::RCODE_INVALID_MSG,"无效的消息"},
                {ResponseCode::RCODE_INVALID_OPTYPE,"无效的操作类型"},
                {ResponseCode::RCODE_INVALID_PARAMS,"无效的参数"},
                {ResponseCode::RCODE_NOT_FOUND_SERVICE,"没有发现服务"},
                {ResponseCode::RCODE_NOT_FOUND_TOPIC,"没有发现主题"},
                {ResponseCode::RCODE_INTERNAL_ERROR,"服务器内部错误"}
            };
            auto it=err_map.find(code);
            if(it==err_map.end())
            {
                return "Unknown";
            }
            return it->second;
        }
    };
}
