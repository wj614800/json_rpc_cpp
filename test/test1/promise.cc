#include<iostream>
#include<future>
#include<thread>

int Add(int a,int b)
{
    std::cout<<"Add Begin"<<std::endl;
    return a+b;
}

int main()
{
    std::promise<int> pro;
    std::future<int> res=pro.get_future();
    std::thread th([&pro](){
        int ret=Add(11,22);
        pro.set_value(ret);
    });

    std::cout<<res.get()<<std::endl;
    th.join();
    return 0;
}