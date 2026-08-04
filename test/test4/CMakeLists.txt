cmake_minimum_required(VERSION 3.18)
project(JsonRpcTest LANGUAGES CXX)
add_executable(register register.cc)
target_include_directories(register PUBLIC ${CMAKE_SOURCE_DIR}/../../source)
target_link_libraries(register PUBLIC jsoncpp)

add_executable(server server.cc)
target_include_directories(server PUBLIC ${CMAKE_SOURCE_DIR}/../../source)
target_link_libraries(server PUBLIC jsoncpp)

add_executable(client client.cc)
target_include_directories(client PUBLIC ${CMAKE_SOURCE_DIR}/../../source)
target_link_libraries(client PUBLIC jsoncpp)


