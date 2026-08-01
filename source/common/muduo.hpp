#pragma once
#include <cassert>
#include <cstring>
#include <ctime>
#include <cstdio>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <sys/timerfd.h>
#include <sys/eventfd.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <signal.h>
#include <iostream>
#include <fstream>
#include <algorithm>
#include <vector>
#include <string>
#include <unordered_map>
#include <regex>
#include <memory>
#include <functional>
#include <thread>
#include <mutex>
#include <atomic>
#include <condition_variable>
#include "detail.hpp"
namespace Muduo
{
    static const uint64_t DEFAULT_BUFFER_SIZE = 1024;
    static const int DEFAULT_BACKLOG = 1024;
    static const int MAX_EVENTS_NUM = 1024;
    static const int DEFAULT_EPOLL_TIMEOUT = -1;
    static const int DEFAULT_INACTIVE_RELEASE_TIME = 60;
    static const int MAX_LINE_LENGTH = 8192;
    static const std::string DEFAULT_HTTP_VERSION="HTTP/1.1";
    enum StatusCode
    {
        // Information responses
        Continue_100 = 100,
        SwitchingProtocol_101 = 101,
        Processing_102 = 102,
        EarlyHints_103 = 103,

        // Successful responses
        OK_200 = 200,
        Created_201 = 201,
        Accepted_202 = 202,
        NonAuthoritativeInformation_203 = 203,
        NoContent_204 = 204,
        ResetContent_205 = 205,
        PartialContent_206 = 206,
        MultiStatus_207 = 207,
        AlreadyReported_208 = 208,
        IMUsed_226 = 226,

        // Redirection messages
        MultipleChoices_300 = 300,
        MovedPermanently_301 = 301,
        Found_302 = 302,
        SeeOther_303 = 303,
        NotModified_304 = 304,
        UseProxy_305 = 305,
        unused_306 = 306,
        TemporaryRedirect_307 = 307,
        PermanentRedirect_308 = 308,

        // Client error responses
        BadRequest_400 = 400,
        Unauthorized_401 = 401,
        PaymentRequired_402 = 402,
        Forbidden_403 = 403,
        NotFound_404 = 404,
        MethodNotAllowed_405 = 405,
        NotAcceptable_406 = 406,
        ProxyAuthenticationRequired_407 = 407,
        RequestTimeout_408 = 408,
        Conflict_409 = 409,
        Gone_410 = 410,
        LengthRequired_411 = 411,
        PreconditionFailed_412 = 412,
        PayloadTooLarge_413 = 413,
        UriTooLong_414 = 414,
        UnsupportedMediaType_415 = 415,
        RangeNotSatisfiable_416 = 416,
        ExpectationFailed_417 = 417,
        ImATeapot_418 = 418,
        MisdirectedRequest_421 = 421,
        UnprocessableContent_422 = 422,
        Locked_423 = 423,
        FailedDependency_424 = 424,
        TooEarly_425 = 425,
        UpgradeRequired_426 = 426,
        PreconditionRequired_428 = 428,
        TooManyRequests_429 = 429,
        RequestHeaderFieldsTooLarge_431 = 431,
        UnavailableForLegalReasons_451 = 451,

        // Server error responses
        InternalServerError_500 = 500,
        NotImplemented_501 = 501,
        BadGateway_502 = 502,
        ServiceUnavailable_503 = 503,
        GatewayTimeout_504 = 504,
        HttpVersionNotSupported_505 = 505,
        VariantAlsoNegotiates_506 = 506,
        InsufficientStorage_507 = 507,
        LoopDetected_508 = 508,
        NotExtended_510 = 510,
        NetworkAuthenticationRequired_511 = 511,
    };

    class Buffer
    {
    private:
        std::vector<char> _buffer;
        std::size_t _read_idx;
        std::size_t _write_idx;

    public:
        Buffer() : _buffer(DEFAULT_BUFFER_SIZE), _read_idx(0), _write_idx(0)
        {
        }

        ~Buffer() = default;

        char *Begin()
        {
            return &(*_buffer.begin());
        }

        char *ReadPosition()
        {
            return Begin() + _read_idx;
        }

        char *WritePosition()
        {
            return Begin() + _write_idx;
        }

        void MoveReadOffset(std::size_t offset)
        {
            assert(offset <= ReadableSize());
            _read_idx += offset;
            if(_read_idx==_write_idx)
            {
                _read_idx=_write_idx=0;
            }
        }

        void MoveWriteOffset(std::size_t offset)
        {
            assert(offset <= TailIdleSize());
            _write_idx += offset;
        }

        std::size_t TailIdleSize() const
        {
            return _buffer.size() - _write_idx;
        }

        std::size_t HeadIdleSize() const
        {
            return _read_idx;
        }

        std::size_t WriteableSize() const
        {
            return HeadIdleSize() + TailIdleSize();
        }

        std::size_t ReadableSize() const
        {
            return _write_idx - _read_idx;
        }

        void EnsureWriteSize(std::size_t size)
        {
            // 后面的空间足够
            if (TailIdleSize() >= size)
                return;

            const std::size_t readable = ReadableSize();
            // 空余的空间足够
            if (WriteableSize() >= size)
            {
                std::copy(ReadPosition(), ReadPosition() + readable, Begin());
                _write_idx -= _read_idx;
                _read_idx = 0;
                return;
            }
            // 空余空间不足，需要扩容
            size_t required_size=std::max(readable+size,_buffer.size()*2);
            std::vector<char> temp(required_size);
            std::copy(ReadPosition(), ReadPosition() + readable, &temp[0]);
            _buffer.swap(temp);
            _write_idx -= _read_idx;
            _read_idx = 0;
        }

        void Write(const void *data, size_t n)
        {
            assert(data != nullptr);
            if (n == 0)
                return;
            EnsureWriteSize(n);
            const char *d = static_cast<const char *>(data);
            std::copy(d, d + n, WritePosition());
        }

        void WriteAndPush(const void *data, size_t n)
        {
            Write(data, n);
            MoveWriteOffset(n);
        }

        void WriteString(const std::string &str)
        {
            Write(str.c_str(), str.size());
        }

        void WriteStringAndPush(const std::string &str)
        {
            WriteAndPush(str.c_str(), str.size());
        }

        void Read(void *data, size_t n)
        {
            assert(n <= ReadableSize());
            assert(data != nullptr);
            std::copy(ReadPosition(), ReadPosition() + n, (char *)data);
        }

        void ReadAndPop(void *data, size_t n)
        {
            Read(data, n);
            MoveReadOffset(n);
        }

        std::string ReadAsString()
        {
            std::string str;
            if (ReadableSize() == 0)
                return str;
            str.resize(ReadableSize());
            Read(&str[0], ReadableSize());
            return str;
        }

        std::string ReadAsStringAndPop()
        {
            std::string str;
            if (ReadableSize() == 0)
                return str;
            str.resize(ReadableSize());
            ReadAndPop(&str[0], ReadableSize());
            return str;
        }

        char* FindCRLF()
        {
           char *ptr = (char *)memchr(ReadPosition(), '\n', ReadableSize());
           return ptr;
        }

        std::string ReadLine()
        {
            char* pos = FindCRLF();
            if(pos==nullptr)
            {
                return "";
            }
            size_t len=pos-ReadPosition()+1;
            std::string line;
            line.resize(len);
            Read(&line[0], len);
            return line;
        }

        std::string ReadLineAndPop()
        {
            std::string line = ReadLine();
            MoveReadOffset(line.size());
            return line;
        }

        void Clear()
        {
            _read_idx = _write_idx = 0;
        }
    };

    class Socket
    {
    private:
        int _sockfd;

    public:
        Socket() : _sockfd(-1)
        {
        }
        Socket(int sockfd) : _sockfd(sockfd)
        {
        }
        Socket(const Socket &) = delete;
        Socket &operator=(const Socket &) = delete;
        ~Socket()
        {
            Close();
        }

        int Fd() const
        {
            return _sockfd;
        }

        int Release()
        {
            int fd = _sockfd;
            _sockfd = -1;
            return fd;
        }

        void Close()
        {
            if (_sockfd >= 0)
            {
                close(_sockfd);
                _sockfd = -1;
            }
        }

        bool Create()
        {
            _sockfd = socket(AF_INET, SOCK_STREAM, 0);
            if (_sockfd < 0)
            {
                LOG_ERROR("create socket failed errno:%d,err_str:%s", errno, strerror(errno));
                return false;
            }
            return true;
        }

        bool Bind(const std::string &ip, uint16_t port)
        {
            struct sockaddr_in local;
            memset(&local, 0, sizeof(local));
            local.sin_family = AF_INET;
            local.sin_port = htons(port);
            inet_pton(AF_INET, ip.c_str(), &local.sin_addr);
            if (bind(_sockfd, (const sockaddr *)&local, sizeof(local)) < 0)
            {
                LOG_ERROR("bind socket failed errno:%d,err_str:%s", errno, strerror(errno));
                return false;
            }
            return true;
        }

        bool Listen(int backlog = DEFAULT_BACKLOG)
        {
            if (listen(_sockfd, backlog) < 0)
            {
                LOG_ERROR("listen socket failed errno:%d,err_str:%s", errno, strerror(errno));
                return false;
            }
            return true;
        }

        int Accept(std::string *ip = nullptr, uint16_t *port = nullptr)
        {
            struct sockaddr_in peer;
            memset(&peer, 0, sizeof(peer));
            socklen_t len = sizeof(peer);
            int fd = accept(_sockfd, (sockaddr *)&peer, &len);
            if (fd < 0)
            {
                LOG_ERROR("socket accept failed errno:%d,err_str:%s", errno, strerror(errno));
                return -1;
            }

            if (ip)
            {
                char ip_str[16];
                memset(ip_str, 0, sizeof(ip_str));
                inet_ntop(AF_INET, &peer.sin_addr, ip_str, sizeof(ip_str));
                *ip = ip_str;
            }

            if (port)
            {
                *port = ntohs(peer.sin_port);
            }

            return fd;
        }

        bool Connect(const std::string &ip, uint16_t port)
        {
            struct sockaddr_in peer;
            memset(&peer, 0, sizeof(peer));
            peer.sin_family = AF_INET;
            peer.sin_port = htons(port);
            inet_pton(AF_INET, ip.c_str(), &peer.sin_addr);
            if (connect(_sockfd, (const sockaddr *)&peer, sizeof(peer)) < 0)
            {
                LOG_ERROR("socket connect failed errno:%d,err_str:%s", errno, strerror(errno));
                return false;
            }
            return true;
        }

        bool SetNonBlock()
        {
            int flag = fcntl(_sockfd, F_GETFL);
            if (flag < 0)
            {
                LOG_ERROR("socket get flag failed,errno:%d,err_str:%s", errno, strerror(errno));
                return false;
            }
            if (fcntl(_sockfd, F_SETFL, flag | O_NONBLOCK) < 0)
            {
                LOG_ERROR("socket set flag failed,errno:%d,err_str:%s", errno, strerror(errno));
                return false;
            }
            return true;
        }

        bool ReuseAddress()
        {
            // int setsockopt(int fd, int leve, int optname, void *val, int vallen)
            int val = 1;
            int n=setsockopt(_sockfd, SOL_SOCKET, SO_REUSEADDR, (void *)&val, sizeof(int));
            if(n<0)
            {
                return false;
            }
            val = 1;
            n=setsockopt(_sockfd, SOL_SOCKET, SO_REUSEPORT, (void *)&val, sizeof(int));
            if(n<0)
            {
                return false;
            }
            return true;
        }

        ssize_t Recv(void *buffer, size_t len, int flag = 0)
        {
            if (len == 0)
                return 0;
            while (true)
            {
                ssize_t n = recv(_sockfd, buffer, len, flag);
                if (n >= 0)
                {
                    return n;
                }
                if (errno == EINTR)
                    continue;
                if (errno != EAGAIN && errno != EWOULDBLOCK)
                {
                    LOG_ERROR("socket receive message failed");
                }
                return -1;
            }
        }

        ssize_t Send(void *buffer, size_t len, int flag = 0)
        {
            if (len == 0)
                return 0;
            ssize_t n = send(_sockfd, buffer, len, flag);
            if (n >= 0)
            {
                return n;
            }
            if (errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK)
            {
                LOG_ERROR("socket send message failed");
            }
            return -1;
        }

        ssize_t NonRecv(void *buffer, size_t len)
        {
            return Recv(buffer, len, MSG_DONTWAIT);
        }

        ssize_t NonSend(void *buffer, size_t len)
        {
            return Send(buffer, len, MSG_DONTWAIT);
        }

        bool CreateServer(const std::string &ip, uint16_t port)
        {
            if (!Create())
            {
                return false;
            }
            if(!ReuseAddress())
            {
                LOG_ERROR("reuse address failed,errno:%d,err_str:%s",errno,strerror(errno));
            }
            if(!SetNonBlock())
            {
                LOG_ERROR("NonBlock set failed,errno:%d,err_str:%s",errno,strerror(errno));
            }
            if (!Bind(ip, port))
            {
                return false;
            }
            if (!Listen())
            {
                return false;
            }
            return true;
        }

        bool CreateClient(const std::string &ip, uint16_t port)
        {
            if (!Create())
                return false;
            if (!Connect(ip, port))
                return false;
            return true;
        }
    };

    class EventLoop;

    class Channel
    {
        using EventCallBack = std::function<void()>;

    private:
        int _fd;
        EventLoop *_loop;
        uint32_t _events;
        uint32_t _revents;
        EventCallBack _read_cb;
        EventCallBack _write_cb;
        EventCallBack _close_cb;
        EventCallBack _error_cb;
        EventCallBack _event_cb;

        std::weak_ptr<void> _tie;
        bool _tied;

    public:
        Channel(int fd, EventLoop *loop) : _fd(fd), _loop(loop), _events(0), _revents(0), _tied(false)
        {
        }

        int Fd() const
        {
            return _fd;
        }

        uint32_t Events() const
        {
            return _events;
        }

        void Tie(const std::shared_ptr<void> &owner)
        {
            _tie = owner;
            _tied = true;
        }

        void SetREvents(uint32_t revents)
        {
            _revents = revents;
        }

        void SetReadCallBack(EventCallBack read_cb)
        {
            _read_cb = read_cb;
        }
        void SetWriteCallBack(EventCallBack write_cb)
        {
            _write_cb = write_cb;
        }
        void SetCloseCallBack(EventCallBack close_cb)
        {
            _close_cb = close_cb;
        }
        void SetErrorCallBack(EventCallBack error_cb)
        {
            _error_cb = error_cb;
        }
        void SetEventCallBack(EventCallBack event_cb)
        {
            _event_cb = event_cb;
        }

        bool ReadAble() const
        {
            return _events & EPOLLIN;
        }

        bool WriteAble() const
        {
            return _events & EPOLLOUT;
        }

        void EnableRead()
        {
            _events |= (EPOLLIN | EPOLLRDHUP);
            Update();
        }

        void EnableWrite()
        {
            _events |= (EPOLLOUT | EPOLLRDHUP);
            Update();
        }

        void DisableRead()
        {
            _events &= ~EPOLLIN;
            Update();
        }

        void DisableWrite()
        {
            _events &= ~EPOLLOUT;
            Update();
        }

        void DisableAll()
        {
            _events = 0;
            Update();
        }

        void Update();

        void Remove();

        void HandleEvent()
        {
            if (_tied)
            {
                auto guard = _tie.lock();
                if (guard)
                {
                    HandleEventWithGuard();
                    return;
                }
                return;
            }
            HandleEventWithGuard();
        }

    private:
        void HandleEventWithGuard()
        {
            uint32_t revents = _revents;
            uint32_t read_events = (EPOLLIN | EPOLLPRI | EPOLLRDHUP);

            if (_event_cb)
                _event_cb();

            if (revents & EPOLLHUP && !(revents & read_events))
            {
                if (_close_cb)
                    _close_cb();
                return;
            }

            if (revents & read_events)
            {
                if (_read_cb)
                    _read_cb();
            }
            if (revents & EPOLLERR)
            {
                if (_error_cb)
                    _error_cb();
                return;
            }
            if (revents & EPOLLOUT)
            {
                if (_write_cb)
                    _write_cb();
            }
        }
    };

    class Poller
    {
    private:
        int _ep_fd;
        struct epoll_event __events[MAX_EVENTS_NUM];
        std::unordered_map<int, Channel *> _channels;

    private:
        bool HasChannel(Channel *channel)
        {
            assert(channel != nullptr);
            int fd = channel->Fd();
            auto it = _channels.find(fd);
            if (it == _channels.end())
            {
                return false;
            }
            return true;
        }

        void Update(Channel *channel, int op)
        {
            int fd = channel->Fd();
            if (op == EPOLL_CTL_DEL)
            {
                epoll_ctl(_ep_fd, EPOLL_CTL_DEL, fd, nullptr);
            }
            else
            {
                struct epoll_event ev;
                ev.events = channel->Events();
                ev.data.fd = fd;
                epoll_ctl(_ep_fd, op, fd, &ev);
            }
        }

    public:
        Poller()
        {
            _ep_fd = epoll_create(MAX_EVENTS_NUM);
            if (_ep_fd < 0)
            {
                LOG_FATAL("epoll create failed errno:%d err_str:%s", errno, strerror(errno));
                exit(1);
            }
        }

        Poller(const Poller &) = delete;
        Poller(Poller &&) = delete;
        Poller &operator=(const Poller &) = delete;
        Poller &operator=(Poller &&) = delete;

        ~Poller()
        {
            close(_ep_fd);
        }

        void UpdateEvent(Channel *channel)
        {
            if (HasChannel(channel))
            {
                Update(channel, EPOLL_CTL_MOD);
            }
            else
            {
                _channels.insert(std::make_pair(channel->Fd(), channel));
                Update(channel, EPOLL_CTL_ADD);
            }
        }

        void RemoveEvent(Channel *channel)
        {
            if (HasChannel(channel))
            {
                _channels.erase(channel->Fd());
                Update(channel, EPOLL_CTL_DEL);
            }
        }

        void Poll(std::vector<Channel *> *channels)
        {
            assert(channels != nullptr);
            int nfds = epoll_wait(_ep_fd, __events, MAX_EVENTS_NUM, DEFAULT_EPOLL_TIMEOUT);
            if (nfds < 0)
            {
                if (errno == EINTR || errno == EAGAIN)
                {
                    return;
                }
                LOG_ERROR("epoll wait failed errno:%d err_str:%s", errno, strerror(errno));
            }
            else if (nfds == 0)
            {
                LOG_INFO("epoll timeout...");
            }
            else
            {
                channels->clear();
                for (int i = 0; i < nfds; i++)
                {
                    auto it = _channels.find(__events[i].data.fd);
                    assert(it != _channels.end());
                    Channel *channel = it->second;
                    channel->SetREvents(__events[i].events);
                    channels->push_back(channel);
                }
            }
        }
    };

    using TaskCallBack = std::function<void()>;
    class TimerTask
    {
    private:
        uint64_t _timer_id;
        bool _cancelled;
        uint32_t _timeout;
        uint64_t _expire_tick;
        TaskCallBack _task_cb;

    public:
        TimerTask(uint64_t timer_id, uint32_t timeout, uint64_t expire_tick) : _timer_id(timer_id), _cancelled(false), _timeout(timeout), _expire_tick(expire_tick)
        {
        }

        uint64_t TimerId() const
        {
            return _timer_id;
        }

        void SetTaskCallBack(TaskCallBack task_cb)
        {
            _task_cb = task_cb;
        }

        uint32_t DelayTime() const
        {
            return _timeout;
        }

        uint64_t ExpireTick() const
        {
            return _expire_tick;
        }

        bool Cancelled() const
        {
            return _cancelled;
        }

        void Cancel()
        {
            _cancelled = true;
        }

        void Refresh(uint64_t tick)
        {
            _expire_tick = tick + _timeout;
        }

        void Run()
        {
            if (_task_cb)
                _task_cb();
        }
    };

    class TimerWheel
    {
        using PtrTask = std::shared_ptr<TimerTask>;

    private:
        uint64_t _tick;
        int _capacity;
        std::vector<std::vector<PtrTask>> _wheel;
        std::unordered_map<uint64_t, PtrTask> _timers;

        EventLoop *_loop;
        int _timer_fd;
        Channel _wheel_channel;

    private:
        static int CreateTimerfd()
        {
            int timer_fd = timerfd_create(CLOCK_MONOTONIC, 0);
            if (timer_fd < 0)
            {
                LOG_FATAL("timerfd create failed errno:%d err_str:%s", errno, strerror(errno));
                exit(2);
            }
            struct itimerspec itime;
            itime.it_interval.tv_sec = 1;
            itime.it_interval.tv_nsec = 0;
            itime.it_value.tv_sec = 1;
            itime.it_value.tv_nsec = 0;
            timerfd_settime(timer_fd, 0, &itime, nullptr);
            return timer_fd;
        }

        uint64_t ReadTimerfd()
        {
            uint64_t times = 0;
            if (read(_timer_fd, &times, 8) < 0)
            {
                LOG_FATAL("read timerfd failed errno:%d,err_str:%s", errno, strerror(errno));
                exit(3);
            }
            return times;
        }

        void RunTimerTask()
        {
            ++_tick;
            int index = _tick % _capacity;
            std::vector<PtrTask> tasks;
            tasks.swap(_wheel[index]);
            for (auto &task : tasks)
            {
                if (task->Cancelled())
                    continue;

                auto it = _timers.find(task->TimerId());
                if (it == _timers.end() || it->second.get() != task.get())
                    continue;

                if (task->ExpireTick() <= _tick)
                {
                    _timers.erase(task->TimerId());
                    task->Run();
                }
                else
                {
                    _wheel[task->ExpireTick() % _capacity].push_back(task);
                }
            }
        }

        void OnTime()
        {
            uint64_t times = ReadTimerfd();
            for (uint64_t time = 0; time < times; time++)
            {
                RunTimerTask();
            }
        }

    public:
        TimerWheel(int capacity, EventLoop *loop) : _tick(0), _capacity(capacity), _wheel(capacity), _loop(loop), _timer_fd(CreateTimerfd()), _wheel_channel(_timer_fd, loop)
        {
            _wheel_channel.SetReadCallBack(std::bind(&TimerWheel::OnTime, this));
            _wheel_channel.EnableRead();
        }

        ~TimerWheel()
        {
            close(_timer_fd);
        }

        void TimerAdd(uint64_t timer_id, uint32_t delay, TaskCallBack task_cb)
        {
            if (delay == 0)
            {
                delay = 1;
            }
            if (HasTimer(timer_id))
            {
                TimerCancel(timer_id);
            }
            uint64_t expire_tick = _tick + delay;
            auto timer = std::make_shared<TimerTask>(timer_id, delay, expire_tick);
            timer->SetTaskCallBack(task_cb);
            int index = expire_tick % _capacity;
            _wheel[index].push_back(timer);
            _timers.insert(std::make_pair(timer_id, timer));
        }

        void TimerRefresh(uint64_t timer_id)
        {
            if (HasTimer(timer_id))
            {
                PtrTask timer = _timers.find(timer_id)->second;
                timer->Refresh(_tick);
            }
        }

        void TimerCancel(uint64_t timer_id)
        {

            auto it = _timers.find(timer_id);
            if (it != _timers.end())
            {
                PtrTask timer = it->second;
                timer->Cancel();
                _timers.erase(it);
            }
        }

        bool HasTimer(uint64_t timer_id)
        {
            auto it = _timers.find(timer_id);
            if (it == _timers.end())
            {
                return false;
            }
            return true;
        }
    };

    class EventLoop
    {
        using Functor = std::function<void()>;

    private:
        std::thread::id _thread_id;
        std::mutex _mutex;
        std::vector<Functor> _tasks_queue;
        int _event_fd;
        Channel _event_channel;
        Poller _poller;
        TimerWheel _timer_wheel;
        std::atomic<bool> _stop;
    private:
        void RunAllTask()
        {
            std::vector<Functor> tasks_queue;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                _tasks_queue.swap(tasks_queue);
            }

            for (Functor task : tasks_queue)
            {
                task();
            }
        }
        static int CreateEventfd()
        {
            int event_fd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
            if (event_fd < 0)
            {
                LOG_FATAL("eventfd create failed errno:%d err_str:%s", errno, strerror(errno));
                exit(3);
            }
            return event_fd;
        }

        void ReadEventfd()
        {
            uint64_t value;
            if (read(_event_fd, &value, sizeof(value)) < 0)
            {
                if (errno == EINTR || errno == EWOULDBLOCK)
                    return;
                LOG_FATAL("eventfd read failed errno:%d err_str:%s", errno, strerror(errno));
                exit(3);
            }
        }

        void WakeUpEventfd()
        {
            uint64_t value = 1;
            if (write(_event_fd, &value, sizeof(value)) < 0)
            {
                if (errno == EINTR || errno == EWOULDBLOCK)
                    return;
                LOG_FATAL("eventfd write failed errno:%d err_str:%s", errno, strerror(errno));
                exit(4);
            }
        }

    public:
        EventLoop() : _thread_id(std::this_thread::get_id()), _event_fd(CreateEventfd()), _event_channel(_event_fd, this), _timer_wheel(600, this),_stop(false)
        {
            _event_channel.SetReadCallBack(std::bind(&EventLoop::ReadEventfd, this));
            _event_channel.EnableRead();
        }

        ~EventLoop()
        {
            close(_event_fd);
        }

        void Start()
        {
            while (!_stop)
            {
                std::vector<Channel *> channels;
                _poller.Poll(&channels);
                for (Channel *channel : channels)
                {
                    if (channel)
                        channel->HandleEvent();
                }

                RunAllTask();
            }
        }

        void Stop()
        {
            QueueInLoop([this](){
                _stop=true;
            });
        }

        bool IsInLoop()
        {
            return _thread_id == std::this_thread::get_id();
        }

        void RunInLoop(Functor task)
        {
            if (IsInLoop())
            {
                task();
            }
            else
            {
                QueueInLoop(task);
            }
        }

        void QueueInLoop(Functor task)
        {
            {
                std::unique_lock<std::mutex> lock(_mutex);
                _tasks_queue.push_back(task);
            }
            WakeUpEventfd();
        }

        void UpdateEvent(Channel *channel)
        {
            _poller.UpdateEvent(channel);
        }

        void RemoveEvent(Channel *channel)
        {
            _poller.RemoveEvent(channel);
        }

        void TimerAdd(uint64_t timer_id, uint32_t delay, TaskCallBack task_cb)
        {
            _timer_wheel.TimerAdd(timer_id, delay, task_cb);
        }

        void TimerRefresh(uint64_t timer_id)
        {
            _timer_wheel.TimerRefresh(timer_id);
        }

        void TimerCancel(uint64_t timer_id)
        {
            _timer_wheel.TimerCancel(timer_id);
        }

        bool HasTimer(uint64_t timer_id)
        {
            return _timer_wheel.HasTimer(timer_id);
        }
    };

    inline void Channel::Update()
    {
        _loop->UpdateEvent(this);
    }

    inline void Channel::Remove()
    {
        _loop->RemoveEvent(this);
    }

    class LoopThread
    {
    private:
        std::condition_variable _cond;
        std::mutex _mutex;
        EventLoop *_loop;
        std::thread _thread;

    private:
        void Entry()
        {
            EventLoop loop;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                _loop = &loop;
                _cond.notify_all();
            }
            loop.Start();
        }

    public:
        LoopThread() : _loop(nullptr), _thread(std::bind(&LoopThread::Entry, this))
        {
        }

        EventLoop *GetLoop()
        {
            EventLoop *loop = nullptr;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                _cond.wait(lock, [&]()
                           { return _loop != nullptr; });
                loop = _loop;
            }
            return loop;
        }
        void Stop()
        {
            if(_loop)_loop->Stop();
            if(_thread.joinable())_thread.join();
        }

        ~LoopThread()
        {
            Stop();
        }
    };

    class LoopThreadPool
    {
    private:
        int _thread_count;
        int _next_idx;
        EventLoop *_base_loop;
        std::vector<LoopThread *> _threads;
        std::vector<EventLoop *> _loops;

    public:
        LoopThreadPool(EventLoop *loop) : _thread_count(0), _next_idx(0), _base_loop(loop)
        {
        }

        void SetThreadCount(int count)
        {
            _thread_count = count;
        }

        void Create()
        {
            _threads.resize(_thread_count);
            _loops.resize(_thread_count);
            for (int i = 0; i < _thread_count; i++)
            {
                _threads[i] = new LoopThread();
                _loops[i] = _threads[i]->GetLoop();
            }
        }

        EventLoop *NextLoop()
        {
            if (_thread_count == 0)
            {
                return _base_loop;
            }
            _next_idx = (_next_idx + 1) % _thread_count;
            return _loops[_next_idx];
        }

        void Stop()
        {
            for(LoopThread* thread:_threads)
            {
                delete thread;
            }
            _threads.clear();
            _loops.clear();
        }

        ~LoopThreadPool()
        {
            Stop();
        }
    };

    class Any
    {
    private:
        class Holder
        {
        public:
            virtual ~Holder() {};
            virtual const std::type_info &type() const = 0;
            virtual Holder *clone() = 0;
        };

        template <class T>
        class PlaceHolder : public Holder
        {
        public:
            T _content;
            PlaceHolder(const T &content) : _content(content)
            {
            }

            virtual const std::type_info &type() const
            {
                return typeid(T);
            }

            virtual Holder *clone()
            {
                return new PlaceHolder(_content);
            }
        };
        Holder *_holder;

    public:
        Any() : _holder(nullptr)
        {
        }

        template <class T>
        Any(const T &content) : _holder(new PlaceHolder<T>(content))
        {
        }

        Any(const Any &other) : _holder(other._holder == nullptr ? nullptr : other._holder->clone())
        {
        }

        ~Any()
        {
            delete _holder;
        }

        Any &swap(Any &other)
        {
            std::swap(_holder, other._holder);
            return *this;
        }

        Any &operator=(Any other)
        {
            other.swap(*this);
            return *this;
        }

        template <class T>
        Any &operator=(const T &content)
        {
            Any(content).swap(*this);
            return *this;
        }

        template <class T>
        T *Get()
        {
            if (_holder == nullptr || typeid(T) != _holder->type())
                return nullptr;
            return &(((PlaceHolder<T> *)_holder)->_content);
        }
    };

    class Connection;
    using PtrConnection = std::shared_ptr<Connection>;

    enum ConnStatus
    {
        CONNECTING,
        CONNECTED,
        DISCONNECTING,
        DISCONNECTED
    };

    class Connection : public std::enable_shared_from_this<Connection>
    {
    private:
        uint64_t _conn_id;
        bool _enable_inactive_release;
        int _sockfd;
        Socket _socket;
        Channel _channel;
        std::atomic<ConnStatus> _status;
        EventLoop *_loop;
        Buffer _in_buffer;
        Buffer _out_buffer;
        Any _context;
        using ConnectedCallBack = std::function<void(const PtrConnection &)>;
        using MessageCallBack = std::function<void(const PtrConnection &, Buffer *)>;
        using ClosedCallBack = std::function<void(const PtrConnection &)>;
        using AnyEventCallBack = std::function<void(const PtrConnection &)>;
        ConnectedCallBack _connected_callback;
        MessageCallBack _message_callback;
        ClosedCallBack _closed_callback;
        AnyEventCallBack _event_callback;
        ClosedCallBack _server_closed_callback;

    private:
        void HandleRead()
        {
            char buffer[DEFAULT_BUFFER_SIZE];
            bool peer_closed = false;
            bool read_error = false;
            while (true)
            {
                ssize_t n = _socket.NonRecv(buffer, sizeof(buffer));
                if (n > 0)
                {
                    _in_buffer.WriteAndPush(buffer, n);
                    continue;
                }
                else if (n == 0)
                {
                    peer_closed = true;
                    break;
                }
                else
                {
                    if (errno == EAGAIN || errno == EWOULDBLOCK)
                    {
                        break;
                    }
                    read_error = true;
                    break;
                }
            }
            if (_in_buffer.ReadableSize() > 0 && _message_callback)
            {
                _message_callback(shared_from_this(), &_in_buffer);
            }
            if (read_error)
            {
                ReleaseInLoop();
                return;
            }
            if (peer_closed)
            {
                ShutdownInLoop();
            }
        }

        void HandleWrite()
        {
            if (_status == DISCONNECTED)
                return;
            ssize_t n = _socket.NonSend(_out_buffer.ReadPosition(), _out_buffer.ReadableSize());
            if (n > 0)
            {
                _out_buffer.MoveReadOffset(n);
            }
            else if (n < 0)
            {
                if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)
                {
                    return;
                }
                ReleaseInLoop();
                return;
            }

            if (_out_buffer.ReadableSize() == 0)
            {
                _channel.DisableWrite();
                if (_status == DISCONNECTING)
                {
                    ReleaseInLoop();
                }
            }
        }

        void HandleClose()
        {
            if (_in_buffer.ReadableSize() > 0)
            {
                if (_message_callback)
                    _message_callback(shared_from_this(), &_in_buffer);
            }
            Release();
        }

        void HandleError()
        {
            HandleClose();
        }

        void HandleEvent()
        {
            if (_enable_inactive_release)
                _loop->TimerRefresh(_conn_id);
            if (_event_callback)
                _event_callback(shared_from_this());
        }

        void EstablishInLoop()
        {
            assert(_status == CONNECTING);
            _status = CONNECTED;
            auto self=shared_from_this();
            _channel.Tie(self);
            _channel.EnableRead();
            if (_connected_callback)
                _connected_callback(self);
        }

        void EnableInactiveReleaseInLoop(uint32_t sec)
        {
            _enable_inactive_release = true;
            if (_loop->HasTimer(_conn_id))
            {
                _loop->TimerRefresh(_conn_id);
            }
            else
            {
                std::weak_ptr<Connection> self = shared_from_this();
                _loop->TimerAdd(_conn_id, sec, [self]()
                                {
                PtrConnection conn=self.lock();
                if(conn)conn->Release(); });
            }
        }

        void ShutdownInLoop()
        {
            if (_status == DISCONNECTING || _status == DISCONNECTED)
                return;
            _status = DISCONNECTING;
            if (_out_buffer.ReadableSize() > 0)
            {
                if (!_channel.WriteAble())
                    _channel.EnableWrite();
            }
            else
            {
                ReleaseInLoop();
            }
        }

        void ReleaseInLoop()
        {
            if (_status == DISCONNECTED)
                return;
            _status = DISCONNECTED;
            _channel.Remove();
            _socket.Close();
            if (_enable_inactive_release)
                CancelInactiveReleaseInLoop();
            if (_closed_callback)
                _closed_callback(shared_from_this());
            if (_server_closed_callback)
                _server_closed_callback(shared_from_this());
        }

        void CancelInactiveReleaseInLoop()
        {
            if (!_enable_inactive_release)
                return;
            _enable_inactive_release = false;
            if (_loop->HasTimer(_conn_id))
            {
                _loop->TimerCancel(_conn_id);
            }
        }

        void SendInLoop(const char *data, size_t len)
        {
            if (_status == DISCONNECTED)
                return;
            _out_buffer.WriteAndPush(data, len);
            if (!_channel.WriteAble())
                _channel.EnableWrite();
        }

        void UpgradeInLoop(const Any &context, const ConnectedCallBack &connected_cb, const MessageCallBack &message_cb, const ClosedCallBack &closed_cb, const AnyEventCallBack &event_cb)
        {
            _context = context;
            _connected_callback = connected_cb;
            _message_callback = message_cb;
            _closed_callback = closed_cb;
            _event_callback = event_cb;
        }

    public:
        Connection(uint64_t conn_id, int sockfd, EventLoop *loop) : _conn_id(conn_id), _enable_inactive_release(false), _sockfd(sockfd), _socket(sockfd), _channel(_sockfd, loop), _status(CONNECTING), _loop(loop)
        {
            _channel.SetReadCallBack(std::bind(&Connection::HandleRead, this));
            _channel.SetWriteCallBack(std::bind(&Connection::HandleWrite, this));
            _channel.SetErrorCallBack(std::bind(&Connection::HandleError, this));
            _channel.SetCloseCallBack(std::bind(&Connection::HandleClose, this));
            _channel.SetEventCallBack(std::bind(&Connection::HandleEvent, this));
        }

        uint64_t ConnId() const
        {
            return _conn_id;
        }

        int Fd() const
        {
            return _sockfd;
        }

        bool Connected()
        {
            return _status == CONNECTED;
        }

        void SetContext(const Any &context)
        {
            _context = context;
        }

        Any *GetContext()
        {
            return &_context;
        }

        void SetConnectedCallBack(const ConnectedCallBack &connected_callback)
        {
            _connected_callback = connected_callback;
        }

        void SetMessageCallBack(const MessageCallBack &message_callback)
        {
            _message_callback = message_callback;
        }

        void SetCloseCallBack(const ClosedCallBack &closed_callback)
        {
            _closed_callback = closed_callback;
        }

        void SetAnyEventCallBack(const AnyEventCallBack &event_callback)
        {
            _event_callback = event_callback;
        }

        void SetServerClosedCallBack(const ClosedCallBack &server_closed_callback)
        {
            _server_closed_callback = server_closed_callback;
        }

        void Establish()
        {
            auto self = shared_from_this();
            _loop->RunInLoop([self]()
                             { self->EstablishInLoop(); });
        }

        void Send(const char *data, size_t len)
        {
            auto message = std::make_shared<std::string>(data, len);
            auto self = shared_from_this();
            _loop->RunInLoop([self, message]()
                             { self->SendInLoop(message->c_str(), message->size()); });
        }

        void Release()
        {
            auto self = shared_from_this();
            _loop->QueueInLoop([self]()
                               { self->ReleaseInLoop(); });
        }

        void Shutdown()
        {
            auto self = shared_from_this();
            _loop->QueueInLoop([self]()
                               { self->ShutdownInLoop(); });
        }

        void EnableInactiveRelease(uint32_t sec)
        {
            auto self = shared_from_this();
            _loop->RunInLoop([self, sec]()
                             { self->EnableInactiveReleaseInLoop(sec); });
        }

        void CancelInactiveRelease()
        {
            auto self = shared_from_this();
            _loop->RunInLoop([self]()
                             { self->CancelInactiveReleaseInLoop(); });
        }

        void Upgrade(const Any &context, const ConnectedCallBack &connected_cb, const MessageCallBack &message_cb, const ClosedCallBack &closed_cb, const AnyEventCallBack &event_cb)
        {
            auto self = shared_from_this();
            _loop->RunInLoop([=]()
                             { self->UpgradeInLoop(context, connected_cb, message_cb, closed_cb, event_cb); });
        }
    };

    class Acceptor
    {
        using AcceptCallBack = std::function<void(int)>;

    private:
        Socket _listen_socket;
        Channel _channel;
        AcceptCallBack _accept_cb;

    private:
        static int CreateServer(const std::string &ip, uint16_t port)
        {
            Socket listen_socket;
            bool ret = listen_socket.CreateServer(ip, port);
            if (ret == false)
            {
                LOG_FATAL("create server failed");
                abort();
            }
            return listen_socket.Release();
        }

        void HandleRead()
        {
            while(true)
            {
                int fd = _listen_socket.Accept();
                if (fd < 0)
                {
                    if(errno==EINTR)continue;
                    if(errno==EAGAIN||errno==EWOULDBLOCK)break;
                    LOG_ERROR("socket accept failed");
                    return;
                }
                Socket socket(fd);
                socket.SetNonBlock();
                socket.Release();
                if (_accept_cb)
                    _accept_cb(fd);
            }
        }

    public:
        Acceptor(const std::string &ip, uint16_t port, EventLoop *base_loop) : _listen_socket(CreateServer(ip, port)), _channel(_listen_socket.Fd(), base_loop)
        {
            _channel.SetReadCallBack(std::bind(&Acceptor::HandleRead, this));
        }

        void SetAcceptorCallBack(const AcceptCallBack &accept_cb)
        {
            _accept_cb = accept_cb;
        }

        void Listen()
        {
            _channel.EnableRead();
        }

        void Stop()
        {
            _channel.Remove();
            _listen_socket.Close();
        }
    };

    class NetWork
    {
    public:
        NetWork()
        {
            signal(SIGPIPE, SIG_IGN);
        }
    };

    class TcpServer
    {
    private:
        uint64_t _next_id;
        bool _enable_inactive_release;
        uint32_t _timeout;
        EventLoop _base_loop;
        Acceptor _acceptor;
        LoopThreadPool _pool;
        std::unordered_map<uint64_t, PtrConnection> _conns;
        using ConnectedCallBack = std::function<void(const PtrConnection &)>;
        using MessageCallBack = std::function<void(const PtrConnection &, Buffer *)>;
        using ClosedCallBack = std::function<void(const PtrConnection &)>;
        using AnyEventCallBack = std::function<void(const PtrConnection &)>;
        using Functor = std::function<void()>;
        ConnectedCallBack _connected_callback;
        MessageCallBack _message_callback;
        ClosedCallBack _closed_callback;
        AnyEventCallBack _event_callback;

        static NetWork _network;

    private:
        void TimerAddInLoop(int delay, const Functor &task)
        {
            ++_next_id;
            _base_loop.TimerAdd(_next_id, delay, task);
        }

        void StopInLoop()
        {
            _acceptor.Stop();
            for(auto& it:_conns)
            {
                it.second->Release();
            }
            _conns.clear();
            _pool.Stop();
            _base_loop.Stop();
        }

        void RemoveConnection(const PtrConnection &conn)
        {
            uint64_t conn_id=conn->ConnId();
            _base_loop.QueueInLoop([this, conn_id]()
                                   { _conns.erase(conn_id); });
        }

        void NewConnection(int fd)
        {
            ++_next_id;
            PtrConnection conn(new Connection(_next_id, fd, _pool.NextLoop()));
            conn->SetConnectedCallBack(_connected_callback);
            conn->SetMessageCallBack(_message_callback);
            conn->SetCloseCallBack(_closed_callback);
            conn->SetAnyEventCallBack(_event_callback);
            conn->SetServerClosedCallBack(std::bind(&TcpServer::RemoveConnection, this, std::placeholders::_1));
            if (_enable_inactive_release)
                conn->EnableInactiveRelease(_timeout);
            conn->Establish();
            _conns.insert(std::make_pair(conn->ConnId(), conn));
        }

    public:
        TcpServer(const std::string &ip, uint16_t port) : _next_id(0), _enable_inactive_release(false), _timeout(0), _base_loop(), _acceptor(ip, port, &_base_loop), _pool(&_base_loop)
        {
            _acceptor.SetAcceptorCallBack(std::bind(&TcpServer::NewConnection, this, std::placeholders::_1));
            _acceptor.Listen();
        }

        void Stop()
        {
            _base_loop.QueueInLoop(std::bind(&TcpServer::StopInLoop,this));
        }

        void TimerAdd(uint32_t delay, const Functor &task)
        {
            _base_loop.RunInLoop(std::bind(&TcpServer::TimerAddInLoop, this, delay, task));
        }

        void SetThreadCount(int thread_count)
        {
            _pool.SetThreadCount(thread_count);
        }

        void SetConnectedCallBack(const ConnectedCallBack &connected_callback)
        {
            _connected_callback = connected_callback;
        }

        void SetMessageCallBack(const MessageCallBack &message_callback)
        {
            _message_callback = message_callback;
        }

        void SetCloseCallBack(const ClosedCallBack &closed_callback)
        {
            _closed_callback = closed_callback;
        }

        void SetAnyEventCallBack(const AnyEventCallBack &event_callback)
        {
            _event_callback = event_callback;
        }

        void EnableInactiveRelease(int sec = DEFAULT_INACTIVE_RELEASE_TIME)
        {
            _enable_inactive_release = true;
            _timeout = sec;
        }

        void Start()
        {
            _pool.Create();
            _base_loop.Start();
        }
    };

    inline NetWork TcpServer::_network;

    class TcpClient
    {
        using ConnectedCallBack = std::function<void(const PtrConnection &)>;
        using MessageCallBack = std::function<void(const PtrConnection &, Buffer *)>;
        using ClosedCallBack = std::function<void(const PtrConnection &)>;
    public:
        TcpClient(const std::string& ip,uint16_t port):_ip(ip),_port(port),_socket(CreateClient()),_conn(new Connection(1,_socket.Fd(),_loop.GetLoop()))
        {
        }

        bool Connect()
        {
            bool ret=_socket.Connect(_ip,_port);
            if(ret)
            {
                _conn->SetMessageCallBack(_message_callback);
                _conn->SetCloseCallBack(_closed_callback);
                _conn->SetConnectedCallBack(_connected_callback);
                _conn->Establish();
            }
            return ret;
        }

        bool Connected()const
        {
            return _conn->Connected();
        }

        void Shutdown()
        {
            _conn->Shutdown();
        }

        PtrConnection GetConnection()
        {
            return _conn;
        }

        void Send(const char* src,size_t len)
        {
            _conn->Send(src,len);
        }

        void SetConnectedCallBack(const ConnectedCallBack &connected_callback)
        {
            _connected_callback = connected_callback;
        }

        void SetMessageCallBack(const MessageCallBack &message_callback)
        {
            _message_callback = message_callback;
        }

        void SetCloseCallBack(const ClosedCallBack &closed_callback)
        {
            _closed_callback = closed_callback;
        }

    private:
        static int CreateClient()
        {
            Socket socket;
            socket.Create();
            return socket.Release();
        }
    private:

        std::string _ip;
        uint16_t _port;
        LoopThread _loop;
        Socket _socket;
        PtrConnection _conn;
        ConnectedCallBack _connected_callback;
        MessageCallBack _message_callback;
        ClosedCallBack _closed_callback;
    };


    class Util
    {
    public:
        static size_t Split(const std::string& src,const std::string& sep,std::vector<std::string>* arr)
        {
            if(arr==nullptr||sep.empty())return 0;
            arr->clear();
            size_t begin=0;
            while(begin<src.size())
            {
                size_t pos=src.find(sep,begin);
                if(pos==src.npos)
                {
                    arr->emplace_back(src.substr(begin));
                    break;
                }
                else
                {
                    if(begin==pos)
                    {
                        begin+=sep.size();
                    }
                    else
                    {
                        arr->emplace_back(src.substr(begin,pos-begin));
                        begin=pos+sep.size();
                    }
                }
            }
            return arr->size();
        }

        static bool ReadFile(const std::string& filename,std::string* content)
        {
            if(content==nullptr)return false;
            std::ifstream ifs(filename,std::ios::binary);
            if(!ifs.is_open())
            {
                LOG_ERROR("%s open failed",filename.c_str());
                return false;
            }
            ifs.seekg(0,std::ios::end);
            size_t size=ifs.tellg();
            ifs.seekg(0,std::ios::beg);
            content->resize(size);
            ifs.read(&(*content)[0],size);
            if(!ifs.good())
            {
                LOG_ERROR("%s read failed",filename.c_str());
                ifs.close();
                return false;
            }
            ifs.close();
            return true;
        }

        static bool WriteFile(const std::string& filename,const std::string& content)
        {
            std::ofstream ofs(filename,std::ios::binary);
            if(!ofs.is_open())
            {
                LOG_ERROR("%s open failed",filename.c_str());
                return false;
            }
            ofs.write(content.c_str(),content.size());
            if(!ofs.good())
            {
                LOG_ERROR("%s write failed",filename.c_str());
                ofs.close();
                return false;
            }
            ofs.close();
            return true;
        }

        static std::string URLEncode(const std::string& url,bool convert_space_to_plus)
        {
            std::string ret;
            for(auto& ch:url)
            {
                if(ch=='.'||ch=='_'||ch=='-'||ch=='~'||isalnum(ch))
                {
                    ret+=ch;
                }
                else if(ch==' '&&convert_space_to_plus)
                {
                    ret+='+';
                }
                else
                {
                    char temp[4];
                    snprintf(temp,sizeof(temp),"%%%02X",ch);
                    temp[3]=0;
                    ret+=temp;
                }
            }
            return ret;
        }

        static int hex_digit_to_int(char c) 
        {
            if (c >= '0' && c <= '9') return c - '0';
            if (c >= 'a' && c <= 'f') return c - 'a' + 10;
            if (c >= 'A' && c <= 'F') return c - 'A' + 10;
            return -1;
        }

        static std::string URLDecode(const std::string& path,bool convert_plus_to_space)
        {
            std::string ret;
            for(size_t i=0;i<path.size();++i)
            {
                if(path[i]=='+'&&convert_plus_to_space)
                {
                    ret+=' ';
                }
                else if(path[i]=='%')
                {
                   
                    char ch=(char)(hex_digit_to_int(path[i+1])*16+hex_digit_to_int(path[i+2]));
                    ret+=ch;
                    i+=2;
                    
                }
                else
                {
                    ret+=path[i];
                }
            }
            return ret;
        }

        static std::string StatusDesc(int status)
        {
            auto it=_statu_msg.find(status);
            if(it==_statu_msg.end())
            {
                return "Unknown";
            }
            return it->second;

        }

        static std::string ExtMime(const std::string& filename)
        {
            auto pos=filename.find_last_of(".");
            if(pos==filename.npos)
            {
                return "application/octet-stream";
            }
            auto it=_mime_msg.find(filename.substr(pos));
            if(it==_mime_msg.end())
            {
                return "application/octet-stream";
            }
            return it->second;
        }

        static bool IsDirectory(const std::string& filepath)
        {
            struct stat st;
            if(stat(filepath.c_str(),&st)<0)
            {
                return false;
            }
            return S_ISDIR(st.st_mode);
        }

        static bool IsRegular(const std::string& filename)
        {
            struct stat st;
            if(stat(filename.c_str(),&st)<0)
            {
                return false;
            }
            return S_ISREG(st.st_mode);
        }

        static bool ValidPath(const std::string& path)
        {
            int level=0;
            std::vector<std::string> arr;
            Split(path,"/",&arr);
            for(auto& str:arr)
            {
                if(str=="..")
                {
                    --level;
                }
                else
                {
                    ++level;
                }
                if(level<0)
                {
                    return false;
                }
            }
            return true;
        }

        static std::string GetHttpDate() 
        {
            std::time_t now = std::time(nullptr);
            std::tm gmt;
            gmtime_r(&now, &gmt);

            char buf[64];
            strftime(buf, sizeof(buf), "%a, %d %b %Y %H:%M:%S GMT", &gmt);
            return std::string(buf);
        }   

      

        static std::string ToUpper(const std::string& str) 
        {
            std::string result = str;
            for (char& c : result) {
                c = std::toupper(static_cast<unsigned char>(c));
            }
            return result;
        }

        private:
            static std::unordered_map<int, std::string> _statu_msg;
            static std::unordered_map<std::string, std::string> _mime_msg;
        };

    inline std::unordered_map<int, std::string> Util::_statu_msg = {
        {100,  "Continue"},
        {101,  "Switching Protocol"},
        {102,  "Processing"},
        {103,  "Early Hints"},
        {200,  "OK"},
        {201,  "Created"},
        {202,  "Accepted"},
        {203,  "Non-Authoritative Information"},
        {204,  "No Content"},
        {205,  "Reset Content"},
        {206,  "Partial Content"},
        {207,  "Multi-Status"},
        {208,  "Already Reported"},
        {226,  "IM Used"},
        {300,  "Multiple Choice"},
        {301,  "Moved Permanently"},
        {302,  "Found"},
        {303,  "See Other"},
        {304,  "Not Modified"},
        {305,  "Use Proxy"},
        {306,  "unused"},
        {307,  "Temporary Redirect"},
        {308,  "Permanent Redirect"},
        {400,  "Bad Request"},
        {401,  "Unauthorized"},
        {402,  "Payment Required"},
        {403,  "Forbidden"},
        {404,  "Not Found"},
        {405,  "Method Not Allowed"},
        {406,  "Not Acceptable"},
        {407,  "Proxy Authentication Required"},
        {408,  "Request Timeout"},
        {409,  "Conflict"},
        {410,  "Gone"},
        {411,  "Length Required"},
        {412,  "Precondition Failed"},
        {413,  "Payload Too Large"},
        {414,  "URI Too Long"},
        {415,  "Unsupported Media Type"},
        {416,  "Range Not Satisfiable"},
        {417,  "Expectation Failed"},
        {418,  "I'm a teapot"},
        {421,  "Misdirected Request"},
        {422,  "Unprocessable Entity"},
        {423,  "Locked"},
        {424,  "Failed Dependency"},
        {425,  "Too Early"},
        {426,  "Upgrade Required"},
        {428,  "Precondition Required"},
        {429,  "Too Many Requests"},
        {431,  "Request Header Fields Too Large"},
        {451,  "Unavailable For Legal Reasons"},
        {501,  "Not Implemented"},
        {502,  "Bad Gateway"},
        {503,  "Service Unavailable"},
        {504,  "Gateway Timeout"},
        {505,  "HTTP Version Not Supported"},
        {506,  "Variant Also Negotiates"},
        {507,  "Insufficient Storage"},
        {508,  "Loop Detected"},
        {510,  "Not Extended"},
        {511,  "Network Authentication Required"}
    };


    inline std::unordered_map<std::string, std::string> Util::_mime_msg = {
    {".aac",        "audio/aac"},
    {".abw",        "application/x-abiword"},
    {".arc",        "application/x-freearc"},
    {".avi",        "video/x-msvideo"},
    {".azw",        "application/vnd.amazon.ebook"},
    {".bin",        "application/octet-stream"},
    {".bmp",        "image/bmp"},
    {".bz",         "application/x-bzip"},
    {".bz2",        "application/x-bzip2"},
    {".csh",        "application/x-csh"},
    {".css",        "text/css"},
    {".csv",        "text/csv"},
    {".doc",        "application/msword"},
    {".docx",       "application/vnd.openxmlformats-officedocument.wordprocessingml.document"},
    {".eot",        "application/vnd.ms-fontobject"},
    {".epub",       "application/epub+zip"},
    {".gif",        "image/gif"},
    {".htm",        "text/html"},
    {".html",       "text/html"},
    {".ico",        "image/vnd.microsoft.icon"},
    {".ics",        "text/calendar"},
    {".jar",        "application/java-archive"},
    {".jpeg",       "image/jpeg"},
    {".jpg",        "image/jpeg"},
    {".js",         "text/javascript"},
    {".json",       "application/json"},
    {".jsonld",     "application/ld+json"},
    {".mid",        "audio/midi"},
    {".midi",       "audio/x-midi"},
    {".mjs",        "text/javascript"},
    {".mp3",        "audio/mpeg"},
    {".mpeg",       "video/mpeg"},
    {".mpkg",       "application/vnd.apple.installer+xml"},
    {".odp",        "application/vnd.oasis.opendocument.presentation"},
    {".ods",        "application/vnd.oasis.opendocument.spreadsheet"},
    {".odt",        "application/vnd.oasis.opendocument.text"},
    {".oga",        "audio/ogg"},
    {".ogv",        "video/ogg"},
    {".ogx",        "application/ogg"},
    {".otf",        "font/otf"},
    {".png",        "image/png"},
    {".pdf",        "application/pdf"},
    {".ppt",        "application/vnd.ms-powerpoint"},
    {".pptx",       "application/vnd.openxmlformats-officedocument.presentationml.presentation"},
    {".rar",        "application/x-rar-compressed"},
    {".rtf",        "application/rtf"},
    {".sh",         "application/x-sh"},
    {".svg",        "image/svg+xml"},
    {".swf",        "application/x-shockwave-flash"},
    {".tar",        "application/x-tar"},
    {".tif",        "image/tiff"},
    {".tiff",       "image/tiff"},
    {".ttf",        "font/ttf"},
    {".txt",        "text/plain"},
    {".vsd",        "application/vnd.visio"},
    {".wav",        "audio/wav"},
    {".weba",       "audio/webm"},
    {".webm",       "video/webm"},
    {".webp",       "image/webp"},
    {".woff",       "font/woff"},
    {".woff2",      "font/woff2"},
    {".xhtml",      "application/xhtml+xml"},
    {".xls",        "application/vnd.ms-excel"},
    {".xlsx",       "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"},
    {".xml",        "application/xml"},
    {".xul",        "application/vnd.mozilla.xul+xml"},
    {".zip",        "application/zip"},
    {".3gp",        "video/3gpp"},
    {".3g2",        "video/3gpp2"},
    {".7z",         "application/x-7z-compressed"}
    };

    struct HttpRequest
    {
        std::string method;
        std::string path;
        std::string version;
        std::string body;
        std::smatch matches;
        std::unordered_map<std::string,std::string> params;
        std::unordered_map<std::string,std::string> headers;
        HttpRequest():version(DEFAULT_HTTP_VERSION)
        {}

        void Reset()
        {
            method.clear();
            path.clear();
            version=DEFAULT_HTTP_VERSION;
            body.clear();
            std::smatch match_temp;
            matches.swap(match_temp);
            params.clear();
            headers.clear();
        }

        bool HasHeader(const std::string& key)const
        {
            auto it=headers.find(key);
            if(it==headers.end())
            {
                return false;
            }
            return true;
        }

        void AddHeader(const std::string& key,const std::string& value)
        {
            headers.insert(std::make_pair(key,value));
        }

        std::string GetHeader(const std::string& key)const
        {
             auto it = headers.find(key);
            if (it == headers.end()) {
                return "";
            }
            return it->second;
        }

        void AddParam(const std::string &key, const std::string &value) 
        {
            params.insert(std::make_pair(key, value));
        }

        //判断是否有某个指定的查询字符串
        bool HasParam(const std::string &key) const 
        {
            auto it = params.find(key);
            if (it == params.end()) {
                return false;
            }
            return true;
        }

        //获取指定的查询字符串
        std::string GetParam(const std::string &key) const 
        {
            auto it = params.find(key);
            if (it == params.end()) {
                return "";
            }
            return it->second;
        }

        size_t ContentLength()const
        {
            if(!HasHeader("Content-Length"))
            {
                return 0;
            }
            std::string value=GetHeader("Content-Length");
            size_t length=0;
            try
            {
               length=std::stoul(value);
            }
            catch(const std::exception& e)
            {
                LOG_ERROR("Content-Length的值不是数字");
                return 0;
            }
            return length;
        }

        bool Close() const 
        {
            // 没有Connection字段，或者有Connection但是值是close，则都是短链接，否则就是长连接
            if (HasHeader("Connection") == true && GetHeader("Connection") == "keep-alive") 
            {
                return false;
            }
            return true;
        }

    };

    struct HttpResponse
    {
        int status;
        bool redirect;
        std::string redirect_url;
        std::string version;
        std::string body;
        std::unordered_map<std::string,std::string> headers;

        HttpResponse():status(OK_200),redirect(false),version(DEFAULT_HTTP_VERSION)
        {}

        void Reset()
        {
            status=OK_200;
            redirect=false;
            redirect_url.clear();
            version=DEFAULT_HTTP_VERSION;
            body.clear();
            headers.clear();
        }

        bool HasHeader(const std::string& key)const
        {
            auto it=headers.find(key);
            if(it==headers.end())
            {
                return false;
            }
            return true;
        }

        void AddHeader(const std::string& key,const std::string& value)
        {
            headers.insert(std::make_pair(key,value));
        }

        std::string GetHeader(const std::string& key)const
        {
             auto it = headers.find(key);
            if (it == headers.end()) {
                return "";
            }
            return it->second;
        }

        void SetStatus(int statuscode)
        {
            status=statuscode;
        }

        void SetContent(const std::string& content,const std::string& type)
        {
            AddHeader("Content-Length",std::to_string(content.size()));
            AddHeader("Content-Type",type);
            body=content;
        }

        void SetRedirect(const std::string& url,int statuscode=Found_302)
        {
            redirect=true;
            redirect_url=url;
            status=statuscode;
            AddHeader("Location",redirect_url);
        }

        bool Close() 
        {
            // 没有Connection字段，或者有Connection但是值是close，则都是短链接，否则就是长连接
            if (HasHeader("Connection") == true && GetHeader("Connection") == "keep-alive") {
                return false;
            }
            return true;
        }

    };

    enum HttpRecvStatus
    {
        RECV_HTTP_ERROR,
        RECV_HTTP_LINE,
        RECV_HTTP_HEADER,
        RECV_HTTP_BODY,
        RECV_HTTP_OVER
    };

    class HttpContext
    {
    private:
        int _resp_status;
        HttpRecvStatus _recv_status;
        HttpRequest _request;
    private:
        void RecvHttpLine(Buffer* buffer)
        {
            if(_recv_status!=RECV_HTTP_LINE)return;
            std::string line=buffer->ReadLineAndPop();
            if(line.empty()&&buffer->ReadableSize()>MAX_LINE_LENGTH)
            {
                _resp_status=UriTooLong_414;
                _recv_status=RECV_HTTP_ERROR;
                return;
            }
            if(line.empty())
            {
                return;
            }
            if(line.size()>MAX_LINE_LENGTH)
            {
                _resp_status=UriTooLong_414;
                _recv_status=RECV_HTTP_ERROR;
                return;
            }

            bool ret=ParseHttpLine(line);
            if(ret==false)
            {
                _recv_status=RECV_HTTP_ERROR;
                return;
            }
            _recv_status=RECV_HTTP_HEADER;
        }

        bool ParseHttpLine(const std::string& line)
        {
            LOG_INFO("%s",line.c_str());
            std::smatch matches;
            std::regex e("(GET|HEAD|POST|PUT|DELETE) ([^?]*)(?:\\?(.*))? (HTTP/1\\.[01])(?:\n|\r\n)?", std::regex::icase);
            bool ret=std::regex_match(line,matches,e);
            if(ret==false)
            {
                _resp_status=BadRequest_400;
                return false;
            }
            _request.method=Util::ToUpper(matches[1]);
            _request.path=Util::URLDecode(matches[2],false);
            _request.version=matches[4];
            std::string query_string=matches[3];
            std::vector<std::string> query_string_arr;
            Util::Split(query_string,"&",&query_string_arr);
            for(auto& str:query_string_arr)
            {
                size_t pos=str.find("=");
                if(pos==str.npos)
                {
                    _resp_status=BadRequest_400;
                    return false;
                }
                std::string key=Util::URLDecode(str.substr(0,pos),true);
                std::string value=Util::URLDecode(str.substr(pos+1),true);
                _request.AddParam(key,value);
            }
            return true;
        }

        void RecvHttpHeader(Buffer* buffer)
        {
            if(_recv_status!=RECV_HTTP_HEADER)return;
            while(true)
            {
                std::string line=buffer->ReadLineAndPop();
                if(line.empty()&&buffer->ReadableSize()>MAX_LINE_LENGTH)
                {
                    _resp_status=RequestHeaderFieldsTooLarge_431;
                    _recv_status=RECV_HTTP_ERROR;
                    return;
                }
                if(line.empty())
                {
                    return;
                }
                if(line.size()>MAX_LINE_LENGTH)
                {
                    _resp_status=RequestHeaderFieldsTooLarge_431;
                    _recv_status=RECV_HTTP_ERROR;
                    return;
                }
                if(line=="\r\n"||line=="\n")
                {
                    break;
                }
                bool ret=ParseHttpHeader(line);
                if(ret==false)
                {
                    _recv_status=RECV_HTTP_ERROR;
                    return;
                }
            }
            _recv_status=RECV_HTTP_BODY;
        }

        bool ParseHttpHeader(std::string& line)
        {
            while(line.back()=='\n')line.pop_back();
            while(line.back()=='\r')line.pop_back();
            std::string sep=": ";
            auto pos=line.find(sep);
            if(pos==line.npos)
            {
                sep=":";
                pos==line.find(sep);
                if(pos==line.npos)
                {
                    _resp_status=BadRequest_400;
                    return false;
                }
            }
            std::string key=line.substr(0,pos);
            std::string value=line.substr(pos+sep.size());
            _request.AddHeader(key,value);
            return true;
        }

        void RecvHttpBody(Buffer* buffer)
        {
            if( _recv_status!=RECV_HTTP_BODY)return;
            size_t length=_request.ContentLength();
            if(buffer->ReadableSize()<length)return;
            std::string& content=_request.body;
            content.resize(length);
            buffer->ReadAndPop(&content[0],length);
            _recv_status=RECV_HTTP_OVER;
        }
    public:
        HttpContext():_resp_status(OK_200),_recv_status(RECV_HTTP_LINE)
        {}

        void Reset()
        {
            _resp_status=OK_200;
            _recv_status=RECV_HTTP_LINE;
            _request.Reset();
        }

        HttpRecvStatus GetRecvStatus()const
        {
            return _recv_status;
        }

        int GetRespStatus()const
        {
            return _resp_status;
        }

        HttpRequest& Request()
        {
            return _request;
        }

        void RecvHttpRequest(Buffer* buffer)
        {
            switch(_recv_status)
            {
                case RECV_HTTP_LINE:
                    RecvHttpLine(buffer);
                case RECV_HTTP_HEADER:
                    RecvHttpHeader(buffer);
                case RECV_HTTP_BODY:
                    RecvHttpBody(buffer);
                default:
                    break;
            }
        }

    };

    class HttpServer
    {
        using Handler=std::function<void(const HttpRequest&,HttpResponse&)>;
        using Handlers=std::vector<std::pair<std::regex,Handler>>;
        using Functor=std::function<void()>;
    private:
        Handlers _get_route;
        Handlers _post_route;
        Handlers _put_route;
        Handlers _delete_route;
        std::string _basedir;
        TcpServer _tcp_server;
    private:
        void ErrorHandler(const HttpRequest& request,HttpResponse& response)
        {
            std::string body;
            body += "<html>";
            body += "<head>";
            body += "<meta http-equiv='Content-Type' content='text/html;charset=utf-8'>";
            body += "</head>";
            body += "<body>";
            body += "<h1>";
            body += std::to_string(response.status);
            body += " ";
            body += Util::StatusDesc(response.status);
            body += "</h1>";
            body += "</body>";
            body += "</html>";
            //2. 将页面数据，当作响应正文，放入rsp中
            response.SetContent(body, "text/html");
        }

        bool IsFileHandler(const HttpRequest& request)
        {
            if(_basedir.empty())
            {
                return false;
            }
            if(request.method!="GET"&&request.method!="HEAD")
            {
                return false;
            }
            std::string filename=_basedir;
            filename+=request.path;
            if(filename.back()=='/')
            {
                filename+="index.html";
            }
            if(!Util::IsRegular(filename))
            {
                return false;
            }
            return true;
        }

        void FileHandler(const HttpRequest& request,HttpResponse& response)
        {
            std::string filename=_basedir;
            filename+=request.path;
            if(filename.back()=='/')
            {
                filename+="index.html";
            }
            bool ret=Util::ValidPath(filename);
            if(ret==false)
            {
                response.SetStatus(Forbidden_403);
                ErrorHandler(request,response);
                return;
            }
            std::string content;
            ret=Util::ReadFile(filename,&content);
            if(ret==false)
            {
                response.SetStatus(InternalServerError_500);
                ErrorHandler(request,response);
                return;
            }
            response.SetContent(content,Util::ExtMime(filename));
        }

        void Dispatcher(const HttpRequest& request,HttpResponse& response,Handlers& route)
        {
            for(auto& e:route)
            {
                std::regex& pattern=e.first;
                if(!std::regex_match(request.path,pattern))
                {
                    continue;
                }
                Handler& handler=e.second;
                handler(request,response);
                return;
            }
            response.SetStatus(NotFound_404);
            ErrorHandler(request,response);
        }

        void Route(const HttpRequest& request,HttpResponse& response)
        {
            if(IsFileHandler(request))
            {
                FileHandler(request,response);
            }
            else if(request.method=="GET"||request.method=="HEAD")
            {
                Dispatcher(request,response,_get_route);
            }
            else if(request.method=="POST")
            {
                Dispatcher(request,response,_post_route);
            }
            else if(request.method=="PUT")
            {
                Dispatcher(request,response,_put_route);
            }
            else if(request.method=="DELETE")
            {
                Dispatcher(request,response,_delete_route);
            }
            else
            {
                response.SetStatus(MethodNotAllowed_405);
                ErrorHandler(request,response);
            }
        }

        void ResponseDecorator(const HttpRequest& request,HttpResponse& response)
        {
            if(response.body.size()>0)
            {
                if(!response.HasHeader("Content-Type"))
                {
                    response.AddHeader("Content-Type","application/octet-stream");
                }
                if(!response.HasHeader("Content-Length"))
                {
                    response.AddHeader("Content-Length",std::to_string(response.body.size()));
                }
            }
            if(response.redirect)
            {
                if(!response.HasHeader("Location"))
                {
                    response.AddHeader("Location",response.redirect_url);
                }
            }
            if(request.Close())
            {
                response.AddHeader("Connection","close");
            }
            else
            {
                response.AddHeader("Connection","keep-alive");
            }
            response.AddHeader("server","MyCppServer/1.0");
            response.AddHeader("Date",Util::GetHttpDate());
        }

        void SendResponse(const PtrConnection& conn,const HttpResponse& response)
        {
            std::stringstream ss;
            ss<<response.version<<" "<<response.status<<" "<<Util::StatusDesc(response.status)<<"\r\n";
            auto& headers=response.headers;
            auto it=headers.begin();
            while(it!=headers.end())
            {
                ss<<it->first<<": "<<it->second<<"\r\n";
                ++it;
            }
            ss<<"\r\n";
            ss<<response.body;
            LOG_INFO("send:\n%s",ss.str().c_str());
            conn->Send(ss.str().c_str(),ss.str().size());
        }
        void OnConnect(const PtrConnection& conn)
        {
            conn->SetContext(HttpContext());
            LOG_INFO("Create a Connection:%d",conn->Fd());
        }
        void OnMessage(const PtrConnection& conn,Buffer* buffer)
        {
            HttpContext* context=conn->GetContext()->Get<HttpContext>();
            while(buffer->ReadableSize()>0)
            {
                context->RecvHttpRequest(buffer);
                HttpRecvStatus recv_status=context->GetRecvStatus();
                HttpRequest request=context->Request();
                HttpResponse response;
                response.SetStatus(context->GetRespStatus());
                if(recv_status!=RECV_HTTP_OVER&&recv_status!=RECV_HTTP_ERROR)
                {
                    return;
                }
                else if(recv_status==RECV_HTTP_ERROR)
                {
                    ErrorHandler(request,response);
                    ResponseDecorator(request,response);
                    SendResponse(conn,response);
                    buffer->Clear();
                    conn->Shutdown();
                    return;
                }
                else if(recv_status==RECV_HTTP_OVER)
                {
                    Route(request,response);
                    ResponseDecorator(request,response);
                    SendResponse(conn,response);
                    context->Reset();
                }
            }
        }
    public:
        HttpServer(const std::string& ip,uint16_t port):_tcp_server(ip,port)
        {
            _tcp_server.SetConnectedCallBack(std::bind(&HttpServer::OnConnect,this,std::placeholders::_1));
            _tcp_server.SetMessageCallBack(std::bind(&HttpServer::OnMessage,this,std::placeholders::_1,std::placeholders::_2));
            _tcp_server.SetCloseCallBack([](const PtrConnection& conn){
                LOG_INFO("close a connect:%d",conn->Fd());
            });
        }

        HttpServer& Get(const std::string& pattern,Handler handler)
        {
            _get_route.emplace_back(std::regex(pattern),handler);
            return *this;
        }
        
        HttpServer& Post(const std::string& pattern,Handler handler)
        {
            _post_route.emplace_back(std::regex(pattern),handler);
            return *this;
        }

        HttpServer& Put(const std::string& pattern,Handler handler)
        {
            _put_route.emplace_back(std::regex(pattern),handler);
            return *this;
        }

        HttpServer& Delete(const std::string& pattern,Handler handler)
        {
            _delete_route.emplace_back(std::regex(pattern),handler);
            return *this;
        }

        bool SetBaseDir(const std::string& basedir)
        {
            if(!Util::IsDirectory(basedir))
            {
                return false;
            }
            _basedir=basedir;
            return true;
        }

        void EnableInactiveRelease(uint32_t timeout)
        {
            _tcp_server.EnableInactiveRelease(timeout);
        }

        void SetThreadCount(int thread_count)
        {
            _tcp_server.SetThreadCount(thread_count);
        }

        void Start()
        {
            _tcp_server.Start();
        }

        void Stop()
        {
            _tcp_server.Stop();
        }

        void TimerAdd(uint32_t delay,const Functor& task)
        {
            _tcp_server.TimerAdd(delay,task);
        }
    };
}
