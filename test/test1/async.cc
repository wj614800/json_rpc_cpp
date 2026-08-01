#include<iostream>
#include<future>
#include<thread>

int Add(int a,int b)
{
    std::this_thread::sleep_for(std::chrono::seconds(1));
    return a+b;
}

int main()
{
    std::future<int> value=std::async(std::launch::async,Add,1,2);
    std::cout<<"主线程执行"<<std::endl;
    std::cout<<value.get()<<std::endl;
}