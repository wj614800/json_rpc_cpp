#pragma once

namespace JsonRpc
{
    #define KEY_METHOD          "method"
    #define KEY_PARAMS          "parameters"
    #define KEY_TOPIC_KEY       "topic_key"
    #define KEY_TOPIC_MSG       "topic_msg"
    #define KEY_OPTYPE          "optype"
    #define KEY_HOST            "host"
    #define KEY_HOST_IP         "ip"
    #define KEY_HOST_PORT       "port"
    #define KEY_RESPONSE_CODE   "rcode"
    #define KEY_RESPONSE_RESULT "result"

    enum class MessageType
    {
        REQUEST_RPC=0,
        RESPONSE_RPC,
        REQUEST_TOPIC,
        RESPONSE_TOPIC,
        REQUEST_SERVICE,
        RESPONSE_SERVICE
    };

    enum class ResponseCode
    {
        RCODE_OK=0,
        RCODE_PARSE_FAILED,
        RCODE_INVALID_MSG,
        RCODE_DISCONNECT,
        RCODE_INVALID_PARAMS,
        RCODE_NOT_FOUND_SERVICE,
        RCODE_INVALID_OPTYPE,
        RCODE_NOT_FOUND_TOPIC,
        RCODE_INTERNAL_ERROR
    };

    enum class ResponseType
    {
        REQUEST_SYNC=0,
        REQUEST_ASYNC,
        REQUEST_CALLBACK
    };

    enum class TopicOptype
    {
        TOPIC_CREATE,
        TOPIC_REMOVE,
        TOPIC_SUBSCRIBE,
        TOPIC_CANCEL,
        TOPIC_PUBLISH
    };

    enum class ServiceOptype
    {
        SERVICE_REGISTER,
        SERVICE_DISCOVER,
        SERVICE_ONLINE,
        SERVICE_OFFLINE
    };
}