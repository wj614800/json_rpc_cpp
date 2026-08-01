#pragma once
#include"../common/fields.hpp"
#include"../common/net.hpp"
#include<future>


namespace JsonRpc
{
    namespace Client
    {
        class Requestor
        {
            using RequestCallBack=std::function<void(const BaseMessage::ptr&)>;
            using AsyncResponse=std::future<BaseMessage::ptr>;

        public:
            using ptr=std::shared_ptr<Requestor>;

            struct RequestDescribe
            {
                using ptr=std::shared_ptr<RequestDescribe>;
                RequestDescribe(const BaseMessage::ptr& req,ResponseType type,RequestCallBack callback=RequestCallBack())
                    :request(req),
                    rtype(type),
                    request_callback(callback)
                {}
                BaseMessage::ptr request;
                std::promise<BaseMessage::ptr> response;
                ResponseType rtype;
                RequestCallBack request_callback;
            };

            void OnResponse(const BaseConnection::ptr& conn,const BaseMessage::ptr& response)
            {
                RequestDescribe::ptr request_describe=GetRequest(response->GetMessageId());
                if(!request_describe)
                {
                    LOG_ERROR("收到了消息，但是找不到请求");
                    conn->Shutdown();
                    return;
                }

                if(request_describe->rtype==ResponseType::REQUEST_ASYNC)
                {
                    request_describe->response.set_value(response);
                }
                else if(request_describe->rtype==ResponseType::REQUEST_CALLBACK)
                {
                    if(request_describe->request_callback)request_describe->request_callback(response);   
                }
                else
                {
                    LOG_ERROR("未知的响应类型");
                }
                RemoveRequest(response->GetMessageId());
            }

            bool Send(const BaseConnection::ptr&conn, const BaseMessage::ptr& request,AsyncResponse& response)
            {
                auto request_describe=NewRequest(request,ResponseType::REQUEST_ASYNC);
                if(!request_describe)
                {
                    LOG_ERROR("构造请求描述失败");
                    return false;
                }
                response=request_describe->response.get_future();
                conn->Send(request);
                return true;
            }

            bool Send(const BaseConnection::ptr&conn, const BaseMessage::ptr& request,const RequestCallBack& cb)
            {
                auto request_describe=NewRequest(request,ResponseType::REQUEST_CALLBACK,cb);
                if(!request_describe)
                {
                    LOG_ERROR("构造请求描述失败");
                    return false;
                }
                conn->Send(request);
                return true;
            }

            bool Send(const BaseConnection::ptr&conn, const BaseMessage::ptr& request,BaseMessage::ptr& response)
            {
                std::future<BaseMessage::ptr> future_response;
                auto request_describe=NewRequest(request,ResponseType::REQUEST_ASYNC);
                if(!request_describe)
                {
                    LOG_ERROR("构造请求描述失败");
                    return false;
                }
                future_response=request_describe->response.get_future();
                conn->Send(request);
                response=future_response.get();
                return true;
            }
        private:
            RequestDescribe::ptr NewRequest(const BaseMessage::ptr& req,ResponseType type,RequestCallBack callback=RequestCallBack())
            {
                auto request_describe=std::make_shared<RequestDescribe>(req,type,callback);
                if(!request_describe)
                {
                    return request_describe;
                }
                {
                    std::unique_lock<std::mutex> lock(_mutex);
                    _requests.insert({req->GetMessageId(),request_describe});
                }
                return request_describe;
            }

            RequestDescribe::ptr GetRequest(const std::string& mid)
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it=_requests.find(mid);
                if(it==_requests.end())
                {
                    return RequestDescribe::ptr();
                }
                return it->second;
            }

            void RemoveRequest(const std::string& mid)
            {
                std::unique_lock<std::mutex> lock(_mutex);
                _requests.erase(mid);
            }
        private:
            std::mutex _mutex;
            std::unordered_map<std::string,RequestDescribe::ptr> _requests;
        };
    }
}