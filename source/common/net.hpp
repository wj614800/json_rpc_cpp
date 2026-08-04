#pragma once
#include"muduo.hpp"
#include"abstract.hpp"
#include"message.hpp"
namespace JsonRpc
{
    class MuduoBuffer:public BaseBuffer
    {
    private:
        friend class BufferFactory;
        MuduoBuffer(Muduo::Buffer* buffer):_buffer(buffer)
        {}
    public:
        using ptr=std::shared_ptr<MuduoBuffer>;
        
        virtual size_t ReadAbleBytes()const override
        {
            return _buffer->ReadableSize();
        }

        virtual int32_t PeekInt32()override
        {
            int32_t value=0;
            _buffer->Read(&value,sizeof(value));
            return value;
        }

        virtual void RetrieveInt32()override
        {
            _buffer->MoveReadOffset(sizeof(int32_t));
        }

        virtual int32_t ReadInt32()override
        {
            int32_t value=PeekInt32();
            RetrieveInt32();
            return value;
        }

        virtual std::string ReadAsString(size_t n)override
        {
            std::string content;
            content.resize(n);
            _buffer->ReadAndPop(&content[0],n);
            return content;
        }

    private:
        Muduo::Buffer* _buffer;
    };

    class BufferFactory
    {
    public:
        template<class ...Args>
        static BaseBuffer::ptr Create(Args&& ...args)
        {
            return std::shared_ptr<MuduoBuffer>(new MuduoBuffer(std::forward<Args>(args)...));
        }
    };

    class LVProtocol:public BaseProtocol
    {
    private:
        friend class ProtocolFactory;
        LVProtocol(){}
    public:
        using ptr=std::shared_ptr<LVProtocol>;
        virtual bool CanProcess(const BaseBuffer::ptr& buffer)override
        {
            if(buffer->ReadAbleBytes()<LenFieldLength)return false;
            int32_t total_len=buffer->PeekInt32();
            if(buffer->ReadAbleBytes()<total_len+LenFieldLength)return false;
            return true;
        }

        virtual bool OnMessage(const BaseBuffer::ptr& buffer,BaseMessage::ptr& message)override
        {
            int32_t total_len=buffer->ReadInt32();
            int32_t mtype=buffer->ReadInt32();
            int32_t id_len=buffer->ReadInt32();
            if(id_len+IdLenFieldLength+MTypeFieldLength>total_len)
            {
                LOG_WARN("RPC消息ID长度非法: total_len=%d id_len=%d",total_len,id_len);
                return false;
            }
            int32_t content_len=total_len-MTypeFieldLength-IdLenFieldLength-id_len;
            std::string mid=buffer->ReadAsString(id_len);
            std::string content=buffer->ReadAsString(content_len);
            message=MessageFactory::Create((MessageType)mtype);
            if(message.get()==nullptr)
            {
                LOG_WARN("RPC消息类型无效: message_type=%d message_id=%s",mtype,mid.c_str());
                return false;
            }
            if(!message->UnSerialize(content))
            {
                return false;
            }
            if(!message->Check())
            {
                return false;
            }
            message->SetMessageId(mid);
            message->SetMessageType((MessageType)mtype);

            return true;
        }

        virtual std::string Serialize(const BaseMessage::ptr& message)override
        {
            std::string content=message->Serialize();
            int32_t mtype=(int32_t)message->GetMessageType();
            std::string mid=message->GetMessageId();
            int32_t id_len=mid.size();
            int32_t total_len=MTypeFieldLength+IdLenFieldLength+id_len+content.size();

            std::string ret;
            ret.reserve(total_len+LenFieldLength);
            ret.append((char*)&total_len,LenFieldLength);
            ret.append((char*)&mtype,MTypeFieldLength);
            ret.append((char*)&id_len,IdLenFieldLength);
            ret.append(mid);
            ret.append(content);
            return ret;
        }
    private:
        static const size_t LenFieldLength=4;
        static const size_t MTypeFieldLength=4;
        static const size_t IdLenFieldLength=4;
    };

    class ProtocolFactory
    {
    public:
        template<class ...Args>
        static BaseProtocol::ptr Create(Args&& ...args)
        {
            return std::shared_ptr<LVProtocol>(new LVProtocol(std::forward<Args>(args)...));
        }
    };

    class MuduoConnection:public BaseConnection
    {
    private:
        friend class ConnectionFactory;
        MuduoConnection(const BaseProtocol::ptr& protocol,const Muduo::PtrConnection& conn):_protocol(protocol),_conn(conn)
        {}
    public:
        using ptr=std::shared_ptr<MuduoConnection>;
        
        virtual void Send(const BaseMessage::ptr& message)override
        {
            std::string body=_protocol->Serialize(message);
            _conn->Send(body.c_str(),body.size());
        }
        virtual void Shutdown()override
        {
            _conn->Shutdown();
        }
        virtual bool Connected()override
        {
            return _conn->Connected();
        }
    private:
        BaseProtocol::ptr _protocol;
        Muduo::PtrConnection _conn;
    };

    class ConnectionFactory
    {
    public:
        template<class ...Args>
        static BaseConnection::ptr Create(Args&& ...args)
        {
            return std::shared_ptr<MuduoConnection>(new MuduoConnection(std::forward<Args>(args)...));
        }
    };

    class MuduoServer:public BaseServer
    {
    private:
        friend class ServerFactory;
        MuduoServer(const std::string ip,uint16_t port):_protocol(ProtocolFactory::Create()),_server(ip,port)
        {
            _server.SetConnectedCallBack(std::bind(&MuduoServer::OnConnection,this,std::placeholders::_1));
            _server.SetCloseCallBack(std::bind(&MuduoServer::OnClose,this,std::placeholders::_1));
            _server.SetMessageCallBack(std::bind(&MuduoServer::OnMessage,this,std::placeholders::_1,std::placeholders::_2));
        }
    public:
        using ptr=std::shared_ptr<MuduoServer>;
       
        virtual void Start()override
        {
            _server.SetThreadCount(5);
            LOG_INFO("RPC传输服务开始运行: worker_threads=%d",5);
            _server.Start();
        }
    private:
        void OnConnection(const Muduo::PtrConnection& conn)
        {
            auto muduo_conn=ConnectionFactory::Create(_protocol,conn);
            if(!muduo_conn)
            {
                LOG_ERROR("创建RPC服务端连接适配器失败: connection_id=%llu",(unsigned long long)conn->ConnId());
                return;
            }
            {
                std::unique_lock<std::mutex> lock(_mutex);
                _conns.insert({conn->ConnId(),muduo_conn});
            }
            LOG_INFO("RPC客户端连接已建立: connection_id=%llu fd=%d",(unsigned long long)conn->ConnId(),conn->Fd());
            if(_connect_cb)_connect_cb(muduo_conn);
        }
        void OnClose(const Muduo::PtrConnection& conn)
        {
            BaseConnection::ptr muduo_conn;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it=_conns.find(conn->ConnId());
                if(it==_conns.end())
                {
                    LOG_WARN("关闭RPC连接时未找到连接记录: connection_id=%llu fd=%d",(unsigned long long)conn->ConnId(),conn->Fd());
                    conn->Shutdown();
                    return;
                }
                muduo_conn=it->second;
                _conns.erase(it);
            }
            LOG_INFO("RPC客户端连接已关闭: connection_id=%llu fd=%d",(unsigned long long)conn->ConnId(),conn->Fd());
            if(_close_cb)_close_cb(muduo_conn);
        }
        void OnMessage(const Muduo::PtrConnection& conn,Muduo::Buffer* buffer)
        {
            BaseBuffer::ptr muduo_buffer=BufferFactory::Create(buffer);
            BaseConnection::ptr muduo_conn;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it=_conns.find(conn->ConnId());
                if(it==_conns.end())
                {
                    LOG_WARN("收到未知RPC连接的数据: connection_id=%llu fd=%d buffered_bytes=%zu",(unsigned long long)conn->ConnId(),conn->Fd(),buffer->ReadableSize());
                    conn->Shutdown();
                    return;
                }
                muduo_conn=it->second;
            }
            while(true)
            {
                if(!_protocol->CanProcess(muduo_buffer))
                {
                    if(muduo_buffer->ReadAbleBytes()>MAX_MESSAGE_SIZE)
                    {
                        LOG_WARN("RPC服务端接收缓冲区超过限制，关闭连接: connection_id=%llu buffered_bytes=%zu limit_bytes=%zu",(unsigned long long)conn->ConnId(),muduo_buffer->ReadAbleBytes(),MAX_MESSAGE_SIZE);
                        return muduo_conn->Shutdown();
                    }
                    break;
                }
                BaseMessage::ptr message;
                if(!_protocol->OnMessage(muduo_buffer,message))
                {
                    muduo_conn->Shutdown();
                }
                if(_message_cb)_message_cb(muduo_conn,message);
            }
        }
    private:
        BaseProtocol::ptr _protocol;
        Muduo::TcpServer _server;
        std::mutex _mutex;
        std::unordered_map<uint64_t,BaseConnection::ptr> _conns;
        static const size_t MAX_MESSAGE_SIZE=8096;
    };

    class ServerFactory
    {
    public:
        template<class ...Args>
        static BaseServer::ptr Create(Args&& ...args)
        {
            return std::shared_ptr<MuduoServer>(new MuduoServer(std::forward<Args>(args)...));
        }
    };


    class MuduoClient:public BaseClient
    {
    private:
        friend class ClientFactory;
        MuduoClient(const std::string ip,uint16_t port):_protocol(ProtocolFactory::Create()),_client(ip,port)
        {
            _client.SetConnectedCallBack(std::bind(&MuduoClient::OnConnection,this,std::placeholders::_1));
            _client.SetCloseCallBack(std::bind(&MuduoClient::OnClose,this,std::placeholders::_1));
            _client.SetMessageCallBack(std::bind(&MuduoClient::OnMessage,this,std::placeholders::_1,std::placeholders::_2));
        }
    public:
        using ptr=std::shared_ptr<MuduoClient>;
        

        virtual bool Connect()override
        {
            return _client.Connect();
        }
        virtual bool Connected()
        {
            return _client.Connected();
        }
        virtual void Shutdown()
        {
            return _client.Shutdown();
        }
        virtual void Send(const BaseMessage::ptr& message)
        {
            std::string content=_protocol->Serialize(message);
            _client.Send(content.c_str(),content.size());
        }

        virtual BaseConnection::ptr Connection()
        {
            auto conn=_client.GetConnection();
            return ConnectionFactory::Create(_protocol,conn);
        }
    private:
        void OnConnection(const Muduo::PtrConnection& conn)
        {
            _muduo_conn=ConnectionFactory::Create(_protocol,conn);
            if(!_muduo_conn)
            {
                LOG_ERROR("创建RPC客户端连接适配器失败: connection_id=%llu",(unsigned long long)conn->ConnId());
                return;
            }
            LOG_INFO("RPC客户端连接就绪: connection_id=%llu fd=%d",(unsigned long long)conn->ConnId(),conn->Fd());
            if(_connect_cb)_connect_cb(_muduo_conn);
        }
        void OnClose(const Muduo::PtrConnection& conn)
        {
            LOG_INFO("RPC客户端连接已断开: connection_id=%llu fd=%d",(unsigned long long)conn->ConnId(),conn->Fd());
            if(_close_cb)_close_cb(_muduo_conn);
        }
        void OnMessage(const Muduo::PtrConnection& conn,Muduo::Buffer* buffer)
        {
            BaseBuffer::ptr muduo_buffer=BufferFactory::Create(buffer);
            while(true)
            {
                if(!_protocol->CanProcess(muduo_buffer))
                {
                    if(muduo_buffer->ReadAbleBytes()>MAX_MESSAGE_SIZE)
                    {
                        LOG_WARN("RPC客户端接收缓冲区超过限制，关闭连接: connection_id=%llu buffered_bytes=%zu limit_bytes=%zu",(unsigned long long)conn->ConnId(),muduo_buffer->ReadAbleBytes(),MAX_MESSAGE_SIZE);
                        return _muduo_conn->Shutdown();
                    }
                    break;
                }
                BaseMessage::ptr message;
                if(!_protocol->OnMessage(muduo_buffer,message))
                {
                   LOG_WARN("RPC客户端解析响应失败，关闭连接: connection_id=%llu buffered_bytes=%zu",(unsigned long long)conn->ConnId(),muduo_buffer->ReadAbleBytes());
                   return _muduo_conn->Shutdown();
                }
                if(_message_cb)_message_cb(_muduo_conn,message);
            }
        }
    private:
        BaseProtocol::ptr _protocol;
        Muduo::TcpClient _client;
        BaseConnection::ptr _muduo_conn;
        static const size_t MAX_MESSAGE_SIZE=8096;
    };

    class ClientFactory
    {
    public:
        template<class ...Args>
        static BaseClient::ptr Create(Args&& ...args)
        {
            return std::shared_ptr<MuduoClient>(new MuduoClient(std::forward<Args>(args)...));
        }
    };
    
}
