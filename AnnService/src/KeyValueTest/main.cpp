#include <iostream>
#include <bits/stdc++.h>

#include "inc/Core/Common.h"
#include "inc/Core/SPANN/ExtraFileController.h"

using namespace SPTAG;

int main(int argc, char* argv[]) {
    SPANN::FileIO fileIO("/nvme0n1/lml/pbfile", 1024 * 1024, std::numeric_limits<SizeType>::max(), 10, 1024, 64, false);
    int max_blocks = 5;
    int kv_num = 1000;
    int dataset_size = 10000;
    int iter_num = 1000;
    std::mutex error_mtx;
    std::vector<std::string> dataset(dataset_size);
    std::vector<int> values(kv_num);
    
    for (int i = 0; i < dataset_size; i++) {
        int size = rand() % (max_blocks << 12);
        dataset[i] = "";
        for (int j = 0; j < size; j++) {
            dataset[i] += (char)(rand() % 256);
        }
    }
    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Data generated\n");

    // 单线程存取
    auto start = std::chrono::high_resolution_clock::now();
    for (int iter = 0; iter < iter_num; iter++) {
        // 随机生成一些数据
        for (int i = 0; i < kv_num; i++) {
            values[i] = rand() % dataset_size;
        }
        // 写入数据
        for (int key = 0; key < kv_num; key++) {
            fileIO.Put(key, dataset[values[key]]);
        }
        // 读取数据
        bool error = false;
        for (int key = 0; key < kv_num; key++) {
            std::string readValue;
            fileIO.Get(key, &readValue);
            if (dataset[values[key]] != readValue) {
                error = true;
                std::cout << "Error: key " << key << " value not match" << std::endl;
                std::cout << "True value: ";
                for (auto c : dataset[values[key]]) {
                    std::cout << (int)c << " ";
                }
                std::cout << std::endl;
                std::cout << "Read value: ";
                for (auto c : readValue) {
                    std::cout << (int)c << " ";
                }
                std::cout << std::endl;
                return 0;
            }
        }
        // if (error) {
        //     std::cout << "Error in single thread iter " << iter << std::endl;
        //     return 0;
        // }
    }
    auto end = std::chrono::high_resolution_clock::now();
    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Single thread test passed\n");
    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Single thread time: %d ms\n", std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count());
    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Single thread IOPS: %fk\n", (double)iter_num * kv_num * 2 / std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count());

    // 多线程存取
    iter_num = 100;
    kv_num = 100000;
    int thread_num = 4;
    values.resize(kv_num);
    start = std::chrono::high_resolution_clock::now();
    for (int iter = 0; iter < iter_num; iter++) {
        std::vector<std::thread> threads;
        std::vector<int> thread_keys[thread_num];
        std::vector<bool> errors(thread_num);
        // 随机生成一些数据
        for (int i = 0; i < kv_num; i++) {
            values[i] = rand() % dataset_size;
        }
        for (int i = 0; i < kv_num; i++) {
            int thread_id = rand() % thread_num;
            thread_keys[thread_id].push_back(i);
        }
        for (int i = 0; i < thread_num; i++) {
            threads.emplace_back([&fileIO, &values, &thread_keys, &dataset, i]() {
                fileIO.Initialize();
                for (auto key : thread_keys[i]) {
                    fileIO.Put(key, dataset[values[key]]);
                }
                fileIO.ExitBlockController();
            });
        }
        for (auto& thread : threads) {
            thread.join();
        }
        threads.clear();
        for (int i = 0; i < thread_num; i++) {
            thread_keys[i].clear();
        }
        for (int i = 0; i < kv_num; i++) {
            int thread_id = rand() % thread_num;
            thread_keys[thread_id].push_back(i);
        }
        for (int i = 0; i < thread_num; i++) {
            threads.emplace_back([&fileIO, &values, &thread_keys, &dataset, &errors, &error_mtx, i]() {
                fileIO.Initialize();
                for (auto key : thread_keys[i]) {
                    std::string readValue;
                    fileIO.Get(key, &readValue);
                    if (dataset[values[key]] != readValue) {
                        // errors[i].push_back({key, readValue});
                        errors[i] = true;
                        std::lock_guard<std::mutex> lock(error_mtx);
                        std::cout << "Error: key " << key << " value not match" << std::endl;
                        std::cout << "True value: ";
                        for (auto c : dataset[values[key]]) {
                            std::cout << (int)c << " ";
                        }
                        std::cout << std::endl;
                        std::cout << "Read value: ";
                        for (auto c : readValue) {
                            std::cout << (int)c << " ";
                        }
                        std::cout << std::endl;
                        return;
                    }
                }
                fileIO.ExitBlockController();
            });
        }
        for (auto& thread : threads) {
            thread.join();
        }
        threads.clear();
        for (int i = 0; i < thread_num; i++) {
            if (errors[i]) {
                std::cout << "Error in multi thread iter " << iter << std::endl;
                return 0;
            }
        }
    }
    end = std::chrono::high_resolution_clock::now();
    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Multi thread test passed\n");
    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Multi thread time: %d ms\n", std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count());
    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Multi thread IOPS: %fk\n", (double)iter_num * kv_num * 2 / std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count());

    // 多线程混合读写存取
    int batch_num = 10;
    iter_num = 100000;
    double read_rate = 0.5;
    values.resize(kv_num);
    start = std::chrono::high_resolution_clock::now();
    for (int batch = 0; batch < batch_num; batch++) {
        std::vector<std::thread> threads;
        std::vector<int> thread_keys[thread_num];
        std::vector<bool> errors(thread_num, false);
        for (int i = 0; i < kv_num; i++) {
            int thread_id = rand() % thread_num;
            thread_keys[thread_id].push_back(i);
        }
        for (int i = 0; i < thread_num; i++) {
            threads.emplace_back([&fileIO, &values, &thread_keys, &dataset, &errors, &error_mtx, i, max_blocks, read_rate, iter_num, dataset_size]() {
                fileIO.Initialize();
                int num = thread_keys[i].size();
                int read_count = 0, write_count = 0;
                bool is_read = false;
                for (auto key : thread_keys[i]) {
                    values[key] = rand() % dataset_size;
                    fileIO.Put(key, dataset[values[key]]);
                }
                if (rand() < RAND_MAX * read_rate) {
                    is_read = true;
                }
                for (int j = 0; j < iter_num; j++) {
                    int key = thread_keys[i][rand() % num];
                    if (is_read) {
                        std::string readValue;
                        fileIO.Get(key, &readValue);
                        if (dataset[values[key]] != readValue) {
                            // errors[i].push_back({key, readValue});
                            errors[i] = true;
                            std::lock_guard<std::mutex> lock(error_mtx);
                            std::cout << "Error: key " << key << " value not match" << std::endl;
                            std::cout << "True value: ";
                            for (auto c : dataset[values[key]]) {
                                std::cout << (int)c << " ";
                            }
                            std::cout << std::endl;
                            std::cout << "Read value: ";
                            for (auto c : readValue) {
                                std::cout << (int)c << " ";
                            }
                            std::cout << std::endl;
                            return;
                        }
                        read_count++;
                    } else {
                        values[key] = rand() % dataset_size; 
                        fileIO.Put(key, dataset[values[key]]);
                        write_count++;
                    }
                }
                fileIO.ExitBlockController();
            });
        }
        for (auto& thread : threads) {
            thread.join();
        }
        threads.clear();
        for (int i = 0; i < thread_num; i++) {
            if (errors[i]) {
                std::cout << "Error in batch " << batch << std::endl;
                return 0;
            }
        }
    }
    end = std::chrono::high_resolution_clock::now();
    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Mix read write test passed\n");
    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Mix read write time: %d ms\n", std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count());
    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Mix read write IOPS: %fk\n", (double)batch_num * iter_num / std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count());
    std::cout << "Test passed" << std::endl;
    return 0;
}