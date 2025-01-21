#include <iostream>
#include <bits/stdc++.h>

#include "inc/Core/Common.h"
#include "inc/Core/SPANN/ExtraFileController.h"

using namespace SPTAG;

int main(int argc, char* argv[]) {
    SPANN::FileIO fileIO("/nvme0n1/lml/testfile", 1024 * 1024, std::numeric_limits<SizeType>::max(), 10, 1024, 64, false);
    std::unordered_map<SizeType, std::string> values;
    int max_blocks = 11;
    int kv_num = 100;

    // 单线程存取
    int iter_num = 1000;
    for (int iter = 0; iter < iter_num; iter++) {
        // 随机生成一些数据
        for (int i = 0; i < kv_num; i++) {
            std::string value;
            int size = rand() % (max_blocks << 12);
            for (int j = 0; j < size; j++) {
                value += (char)(rand() % 256);
            }
            values[i] = value;
        }
        // 写入数据
        for (auto& [key, value] : values) {
            fileIO.Put(key, value);
        }
        // 读取数据
        bool error = false;
        for (auto& [key, value] : values) {
            std::string readValue;
            fileIO.Get(key, &readValue);
            if (value != readValue) {
                error = true;
                std::cout << "Error: key " << key << " value not match" << std::endl;
                std::cout << "True value: ";
                for (auto c : value) {
                    std::cout << (int)c << " ";
                }
                std::cout << std::endl;
                std::cout << "Read value: ";
                for (auto c : readValue) {
                    std::cout << (int)c << " ";
                }
                std::cout << std::endl;
            }
        }
        if (error) {
            std::cout << "Error in single thread iter " << iter << std::endl;
            return 0;
        }
    }

    // 多线程存取
    iter_num = 1000;
    for (int iter = 0; iter < iter_num; iter++) {
        int thread_num = 4;
        std::vector<std::thread> threads;
        std::vector<int> thread_keys[thread_num];
        std::vector<std::vector<std::pair<int, std::string>>> errors(thread_num);
        // 随机生成一些数据
        for (int i = 0; i < kv_num; i++) {
            std::string value;
            int size = rand() % (max_blocks << 12);
            for (int j = 0; j < size; j++) {
                value += (char)(rand() % 256);
            }
            values[i] = value;
        }
        for (int i = 0; i < kv_num; i++) {
            int thread_id = rand() % thread_num;
            thread_keys[thread_id].push_back(i);
        }
        for (int i = 0; i < thread_num; i++) {
            threads.emplace_back([&fileIO, &values, &thread_keys, i]() {
                for (auto key : thread_keys[i]) {
                    fileIO.Put(key, values[key]);
                }
            });
        }
        for (auto& thread : threads) {
            thread.join();
        }
        threads.clear();
        for (int i = 0; i < thread_num; i++) {
            threads.emplace_back([&fileIO, &values, &thread_keys, &errors, i]() {
                for (auto key : thread_keys[i]) {
                    std::string readValue;
                    fileIO.Get(key, &readValue);
                    if (values[key] != readValue) {
                        errors[i].push_back({key, readValue});
                    }
                }
            });
        }
        for (auto& thread : threads) {
            thread.join();
        }
        bool error = false;
        for (int i = 0; i < thread_num; i++) {
            if (errors[i].size() > 0) {
                std::cout << "Thread " << i << "error values: " << std::endl;
                for (auto key : errors[i]) {
                    std::cout << "Error key " << key.first << " value not match" << std::endl;
                    std::cout << "True value: ";
                    for (auto c : values[key.first]) {
                        std::cout << (int)c << " ";
                    }
                    std::cout << std::endl;
                    std::cout << "Read value: ";
                    for (auto c : key.second) {
                        std::cout << (int)c << " ";
                    }
                    std::cout << std::endl;
                }
                error = true;
            }
        }
        if (error) {
            std::cout << "Error in multi thread iter " << iter << std::endl;
            return 0;
        }
    }
    std::cout << "Test passed" << std::endl;
    return 0;
}