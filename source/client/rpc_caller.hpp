#pragma once
#include"requestor.hpp"

namespace JsonRpc
{
    namespace Client
    {
        class RpcCaller
        {
        public:
            using ptr=std::shared_ptr<RpcCaller>;
            using AsyncResponse=std::future<Json::Value>;
            using RequestCallBack=std::function<void(const BaseMessage::ptr&)>;
            RpcCaller(const Requestor::ptr& requestor):_requestor(requestor)
            {}
            //同步调用
            bool Call(const BaseConnection::ptr& conn,const std::string& method,const Json::Value& params,Json::Value& result)
            {
                auto request=MessageFactory::Create<RpcRequest>();
                request->SetMessageType(MessageType::REQUEST_RPC);
                request->SetMessageId(Util::UUID());
                request->SetMethod(method);
                request->SetParams(params);
                BaseMessage::ptr response;
                if(!_requestor->Send(conn,request,response))
                {
                    LOG_ERROR("同步发送失败");
                    return false;
                }
                if(!response||response->GetMessageType()!=MessageType::RESPONSE_RPC)
                {
                    LOG_ERROR("结果接受失败");
                    return false;
                }

                RpcResponse::ptr rpc_response=std::dynamic_pointer_cast<RpcResponse>(response);
                if(!rpc_response)
                {
                    LOG_ERROR("响应转化失败");
                    return false;
                }
                if(rpc_response->GetResponseCode()!=ResponseCode::RCODE_OK)
                {
                    LOG_ERROR("%s",Util::ErrorReason(rpc_response->GetResponseCode()).c_str());
                    return false;
                }
                result=rpc_response->GetResponseResult();
                return true;

            }
            //异步调用
            bool Call(const BaseConnection::ptr& conn,const std::string& method,const Json::Value& params,AsyncResponse& result)
            {
                auto request=MessageFactory::Create<RpcRequest>();
                request->SetMessageType(MessageType::REQUEST_RPC);
                request->SetMessageId(Util::UUID());
                request->SetMethod(method);
                request->SetParams(params);

                std::shared_ptr<std::promise<Json::Value>> response=std::make_shared<std::promise<Json::Value>>();
                result=response->get_future();
                RequestCallBack cb=std::bind(&RpcCaller::AsyncCallBack,this,response,std::placeholders::_1);
                if(!_requestor->Send(conn,request,cb))
                {
                    LOG_ERROR("异步发送失败");
                    return false;
                }
                return true;
            }

            bool Call(const BaseConnection::ptr& conn,const std::string& method,const Json::Value& params,const RequestCallBack& callback)
            {
                auto request=MessageFactory::Create<RpcRequest>();
                request->SetMessageType(MessageType::REQUEST_RPC);
                request->SetMessageId(Util::UUID());
                request->SetMethod(method);
                request->SetParams(params);

                if(!_requestor->Send(conn,request,callback))
                {
                    LOG_ERROR("回调发送失败");
                    return false;
                }
                return true;
            }
        private:
            void AsyncCallBack(const std::shared_ptr<std::promise<Json::Value>>& response,const BaseMessage::ptr& message)
            {
                if(!message||message->GetMessageType()!=MessageType::RESPONSE_RPC)
                {
                    LOG_ERROR("结果接受失败");
                    response->set_value(Json::Value());
                    return;
                }
                RpcResponse::ptr rpc_message=std::dynamic_pointer_cast<RpcResponse>(message);
                if(!rpc_message)
                {
                    LOG_ERROR("响应转化失败");
                    response->set_value(Json::Value());
                    return;
                }
                if(rpc_message->GetResponseCode()!=ResponseCode::RCODE_OK)
                {
                    LOG_ERROR("%s",Util::ErrorReason(rpc_message->GetResponseCode()).c_str());
                    response->set_value(Json::Value());
                    return;
                }
                response->set_value(rpc_message->GetResponseResult());
            }

        private:
            Requestor::ptr _requestor;
        };
    }
}