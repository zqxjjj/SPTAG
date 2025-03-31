#ifndef _SPTAG_SPANN_EXTRAFILECONTROLLER_H_
#define _SPTAG_SPANN_EXTRAFILECONTROLLER_H_
#define USE_ASYNC_IO
// #define USE_FILE_DEBUG
#include "inc/Helper/KeyValueIO.h"
#include "inc/Core/Common/Dataset.h"
#include "inc/Core/VectorIndex.h"
#include "inc/Helper/ThreadPool.h"
#include <cstdlib>
#include <memory>
#include <atomic>
#include <mutex>
#include <shared_mutex>
#include <fcntl.h>
#include <tbb/concurrent_queue.h>
#include <tbb/concurrent_hash_map.h>
#include <sys/syscall.h>
#include <linux/aio_abi.h>
#include <list>
namespace SPTAG::SPANN {
    typedef std::int64_t AddressType;
    class FileIO : public Helper::KeyValueIO {
        class BlockController {
        private:
            static constexpr const char* kFileIoPath = "SPFRESH_FILE_IO_PATH";
            static char* filePath;
            static int fd;

            static constexpr AddressType kSsdImplMaxNumBlocks = (300ULL << 30) >> PageSizeEx; // 300G
            static constexpr const char* kFileIoDepth = "SPFRESH_FILE_IO_DEPTH";
            static constexpr int kSsdFileIoDefaultIoDepth = 1024;
            static constexpr const char* kFileIoThreadNum = "SPFRESH_FILE_IO_THREAD_NUM";
            static constexpr int kSsdFileIoDefaultIoThreadNum = 64;
            static constexpr const char* kFileIoAlignment = "SPFRESH_FILE_IO_ALIGNMENT";
            static constexpr int kSsdFileIoDefaultAlignment = 4096;

            tbb::concurrent_queue<AddressType> m_blockAddresses;
            tbb::concurrent_queue<AddressType> m_blockAddresses_reserve;
            
            pthread_t m_fileIoTid;
            pthread_t m_ioStatisticsTid;
            volatile bool m_fileIoThreadStartFailed = false;
            volatile bool m_fileIoThreadReady = false;
            volatile bool m_fileIoThreadExiting = false;

            int m_ssdFileIoAlignment = kSsdFileIoDefaultAlignment;
            int m_ssdFileIoDepth = kSsdFileIoDefaultIoDepth;
            int m_ssdFileIoThreadNum = kSsdFileIoDefaultIoThreadNum;
            struct SubIoRequest {
                struct iocb myiocb;
                AddressType real_size;
                AddressType offset;
                void* app_buff;
                BlockController* ctrl;
                int posting_id;
            };
            tbb::concurrent_queue<SubIoRequest *> m_submittedSubIoRequests;
            struct IoContext {
                std::vector<SubIoRequest> sub_io_requests;
                std::queue<SubIoRequest *> free_sub_io_requests;
                int in_flight = 0;
            };
            static thread_local struct IoContext m_currIoContext;
            static thread_local int debug_fd;
            static thread_local uint64_t iocp;
            static std::chrono::high_resolution_clock::time_point m_startTime;

            static thread_local int id;
            int m_maxId = 0;
            std::queue<int> m_idQueue;
            std::vector<int> read_complete_vec;
            std::vector<int> read_submit_vec;
            std::vector<int> write_complete_vec;
            std::vector<int> write_submit_vec;
            std::vector<int64_t> read_bytes_vec;
            std::vector<int64_t> write_bytes_vec;
            std::vector<int64_t> read_blocks_time_vec;

            std::mutex m_uniqueResourceMutex;

            static int m_ssdInflight;

            static std::unique_ptr<char[]> m_memBuffer;

            std::mutex m_initMutex;
            int m_numInitCalled = 0;

            int m_batchSize;
            static std::atomic<int> m_ioCompleteCount;
            int m_preIOCompleteCount = 0;
            int64_t m_preIOBytes = 0;
            std::chrono::time_point<std::chrono::high_resolution_clock> m_preTime = std::chrono::high_resolution_clock::now();

            static void* InitializeFileIo(void* args);

            static void* IoStatisticsThread(void* args) {
                auto ctrl = static_cast<BlockController*>(args);
                pthread_exit(NULL);
            };

            // static void Start(void* args);

            // static void FileIoLoop(void *arg);

            // static void FileIoCallback(bool success, void *cb_arg);

            // static void Stop(void* args);

        public:
            bool Initialize(int batchSize);

            bool GetBlocks(AddressType* p_data, int p_size);

            bool ReleaseBlocks(AddressType* p_data, int p_size);

            bool ReadBlocks(AddressType* p_data, std::string* p_value, const std::chrono::microseconds &timeout = std::chrono::microseconds::max());

            bool ReadBlocks(AddressType* p_data, ByteArray& p_value, const std::chrono::microseconds &timeout = std::chrono::microseconds::max());

            bool ReadBlocks(const std::vector<AddressType*>& p_data, std::vector<std::string>* p_value, const std::chrono::microseconds &timeout = std::chrono::microseconds::max());

            bool ReadBlocks(const std::vector<AddressType*>& p_data, std::vector<ByteArray>& p_value, const std::chrono::microseconds &timeout = std::chrono::microseconds::max());

            bool WriteBlocks(AddressType* p_data, int p_size, const std::string& p_value);

            bool WriteBlocks(AddressType* p_data, int p_size, const ByteArray& p_value);

            bool IOStatistics();

            bool ShutDown();

            int RemainBlocks() {
                return m_blockAddresses.unsafe_size();
            };

            ErrorCode Checkpoint(std::string prefix) {
                std::string filename = prefix + "_blockpool";
                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "FileIO: saving block pool\n");
                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Reload reserved blocks!\n");
                AddressType currBlockAddress = 0;
                for (int count = 0; count < m_blockAddresses_reserve.unsafe_size(); count++) {
                    m_blockAddresses_reserve.try_pop(currBlockAddress);
                    m_blockAddresses.push(currBlockAddress);
                }
                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Reload Finish!\n");
                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Save blockpool To %s\n", filename.c_str());
                auto ptr = f_createIO();
                if (ptr == nullptr || !ptr->Initialize(filename.c_str(), std::ios::binary | std::ios::out)) return ErrorCode::FailedCreateFile;
                int blocks = RemainBlocks();
                IOBINARY(ptr, WriteBinary, sizeof(SizeType), (char*)&blocks);
                for (auto it = m_blockAddresses.unsafe_begin(); it != m_blockAddresses.unsafe_end(); it++) {
                    IOBINARY(ptr, WriteBinary, sizeof(AddressType), (char*)&(*it));
                }
                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Save Finish!\n");
                return ErrorCode::Success;
            }

            ErrorCode Recovery(std::string prefix, int batchSize) {
                std::lock_guard<std::mutex> lock(m_initMutex);
                m_numInitCalled++;
                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "FileIO Recovery: Loading block pool\n");
                std::string filename = prefix + "_blockpool";
                auto ptr = f_createIO();
                if (ptr == nullptr || !ptr->Initialize(filename.c_str(), std::ios::binary | std::ios::in)) {
                    return ErrorCode::FailedCreateFile;
                }
                int blocks;
                IOBINARY(ptr, ReadBinary, sizeof(SizeType), (char*)&blocks);
                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "FileIO Recovery: Reading %d blocks to pool\n", blocks);
                AddressType currBlockAddress = 0;
                for (int i = 0; i < blocks; i++) {
                    IOBINARY(ptr, ReadBinary, sizeof(AddressType), (char*)&(currBlockAddress));
                    m_blockAddresses.push(currBlockAddress);
                }

                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "FileIO Recovery: Initializing FileIO\n");
                
                if (m_numInitCalled == 1) {
                    m_batchSize = batchSize;
                    pthread_create(&m_fileIoTid, NULL, &InitializeFileIo, this);
                    while(!m_fileIoThreadReady && !m_fileIoThreadStartFailed);
                    if (m_fileIoThreadStartFailed) {
                        fprintf(stderr, "SPDKIO::BlockController::Initialize failed\n");
                        return ErrorCode::Fail;
                    }
                }
                // Create sub I/O request pool
                m_currIoContext.sub_io_requests.resize(m_ssdFileIoDepth);
                m_currIoContext.in_flight = 0;
                for (auto &sr : m_currIoContext.sub_io_requests) {
                    sr.app_buff = nullptr;
                    auto buf_ptr = aligned_alloc(m_ssdFileIoAlignment, PageSize);
                    if (buf_ptr == nullptr) {
                        fprintf(stderr, "FileIO::BlockController::Initialize failed: aligned_alloc failed\n");
                        return ErrorCode::Fail;
                    }
                    sr.myiocb.aio_buf = reinterpret_cast<uint64_t>(buf_ptr);
                    sr.myiocb.aio_fildes = fd;
                    sr.myiocb.aio_data = reinterpret_cast<uintptr_t>(&sr);
                    sr.myiocb.aio_nbytes = PageSize;
                    sr.ctrl = this;
                    m_currIoContext.free_sub_io_requests.push(&sr);
                }
                return ErrorCode::Success;
            }
        };

        class CompactionJob : public Helper::ThreadPool::Job
        {
        private:
            FileIO* m_fileIO;

        public:
            CompactionJob(FileIO* fileIO): m_fileIO(fileIO) {}

            ~CompactionJob() {}

            inline void exec(IAbortOperation* p_abort) override {
                m_fileIO->ForceCompaction();
            }
        };

        class LRUCache {
            int capacity;   // Page Num
            std::list<SizeType> keys;  // Page Address
            std::unordered_map<SizeType, std::pair<std::vector<void *>, std::list<SizeType>::iterator>> cache;    // Page Address -> Page Address in Cache
            std::queue<void *> free_pages; // Free Page Address
            std::mutex mu;

        public:
            LRUCache(int capacity) {
                this->capacity = capacity;
                for (int i = 0; i < capacity; i++) {
                    free_pages.push((void *)malloc(PageSize));
                }
            }

            ~LRUCache() {
                while (!free_pages.empty()) {
                    free(free_pages.front());
                    free_pages.pop();
                }
            }

            bool get(SizeType key, void* value, AddressType size) {
                mu.lock();
                auto it = cache.find(key);
                if (it == cache.end()) {
                    mu.unlock();
                    return false;  // 如果键不存在，返回 -1
                }
                // 更新访问顺序，将该键移动到链表头部
                AddressType offset = 0;
                for (auto addr : it->second.first) {
                    auto actual_size = size - offset < PageSize ? size - offset : PageSize;
                    memcpy(value + offset, addr, actual_size);
                    offset += PageSize;
                }
                keys.splice(keys.begin(), keys, it->second.second);
                it->second.second = keys.begin();
                mu.unlock();
                return true;
            }

            bool put(SizeType key, void* value, AddressType size) {
                mu.lock();
                auto it = cache.find(key);
                if (it != cache.end()) {
                    auto keys_it = it->second.second;
                    keys.splice(keys.begin(), keys, keys_it);
                    it->second.second = keys.begin();
                    keys_it = keys.begin();
                    auto delta_size = size - it->second.first.size() * PageSize;
                    while (free_pages.size() * PageSize < delta_size && (keys.size() > 1)) {
                        auto last = keys.back();
                        auto it = cache.find(last);
                        for (auto addr : it->second.first) {
                            free_pages.push(addr);
                        }
                        cache.erase(it);
                        keys.pop_back();
                    }
                    if (free_pages.size() * PageSize < delta_size) {
                        mu.unlock();
                        return false;
                    }
                    for (int i = 0; i < it->second.first.size(); i++) {
                        auto real_size = size - i * PageSize < PageSize ? size - i * PageSize : PageSize;
                        memcpy(it->second.first[i], value + i * PageSize, real_size);
                    }
                    for (int i = 0; i < delta_size / PageSize + (delta_size % PageSize == 0 ? 0 : 1); i++) {
                        auto offset = it->second.first.size() * PageSize;
                        it->second.first.push_back(free_pages.front());
                        auto real_size = size - offset < PageSize ? size - offset : PageSize;
                        memcpy(it->second.first.back(), value + offset, real_size);
                        free_pages.pop();
                    }
                    mu.unlock();
                    return true;
                }
                while (free_pages.size() * PageSize < size && (!keys.empty())) {
                    auto last = keys.back();
                    auto it = cache.find(last);
                    for (auto addr : it->second.first) {
                        free_pages.push(addr);
                    }
                    cache.erase(it);
                    keys.pop_back();
                }
                if (free_pages.size() * PageSize < size) {
                    mu.unlock();
                    return false;
                }
                auto keys_it = keys.insert(keys.begin(), key);
                std::pair<std::vector<void *>, std::list<SizeType>::iterator> value_pair;
                value_pair.first.resize(size / PageSize + (size % PageSize == 0 ? 0 : 1));
                for (int i = 0; i < value_pair.first.size(); i++) {
                    value_pair.first[i] = free_pages.front();
                    auto real_size = size - i * PageSize < PageSize ? size - i * PageSize : PageSize;
                    memcpy(value_pair.first[i], value + i * PageSize, real_size);
                    free_pages.pop();
                }
                value_pair.second = keys_it;
                cache[key] = value_pair;
                mu.unlock();
                return true;
            }

            bool del(SizeType key) {
                mu.lock();
                auto it = cache.find(key);
                if (it == cache.end()) {
                    mu.unlock();
                    return false;  // 如果键不存在，返回 false
                }
                for (auto addr : it->second.first) {
                    free_pages.push(addr);
                }
                keys.erase(it->second.second);
                cache.erase(it);
                mu.unlock();
                return true;
            }

            bool merge(SizeType key, void* value, AddressType size, AddressType original_size) {
                mu.lock();
                auto it = cache.find(key);
                if (it == cache.end()) {
                    mu.unlock();
                    return false;  // 如果键不存在，返回 false
                }
                if (original_size + size > capacity * PageSize) {
                    for (auto addr : it->second.first) {
                        free_pages.push(addr);
                    }
                    keys.erase(it->second.second);
                    cache.erase(it);
                    mu.unlock();
                    return false;
                }
                auto keys_it = it->second.second;
                keys.splice(keys.begin(), keys, keys_it);
                it->second.second = keys.begin();
                keys_it = keys.begin();

                // 处理最后一个块没写满的情况
                if (original_size % PageSize > 0) {
                    auto rest_size = original_size % PageSize;
                    memcpy(it->second.first.back() + rest_size, value, PageSize - rest_size);
                    value += PageSize - rest_size;
                    original_size += PageSize - rest_size;
                    size -= PageSize - rest_size;
                }

                while (free_pages.size() * PageSize < size && (keys.size() > 1)) {
                    auto last = keys.back();
                    auto it = cache.find(last);
                    for (auto addr : it->second.first)
                        free_pages.push(addr);
                    cache.erase(it);
                    keys.pop_back();
                }
                
                for (int i = 0; i < size / PageSize; i++) {
                    it->second.first.push_back(free_pages.front());
                    memcpy(it->second.first.back(), value, PageSize);
                    free_pages.pop();
                    value += PageSize;
                    original_size += PageSize;
                }
                if (size % PageSize > 0) {
                    it->second.first.push_back(free_pages.front());
                    memcpy(it->second.first.back(), value, size % PageSize);
                    free_pages.pop();
                    original_size += size % PageSize;
                }
                mu.unlock();
                return true;
            }
        }; 

    public:
        FileIO(const char* filePath, SizeType blockSize, SizeType capacity, SizeType postingBlocks, SizeType bufferSize = 1024, int batchSize = 64, bool recovery = false, int compactionThreads = 1) {
            // TODO: 后面还得再看看，可能需要修改
            m_mappingPath = std::string(filePath);
            m_blockLimit = postingBlocks + 1;
            m_bufferLimit = bufferSize;

            const char* fileIoUseLock = getenv(kFileIoUseLock);
            if(fileIoUseLock) {
                if(strcmp(fileIoUseLock, "True") == 0) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "FileIO: Using lock\n");
                    m_fileIoUseLock = true;
                    const char* fileIoLockSize = getenv(kFileIoLockSize);
                    if(fileIoLockSize) {
                        m_fileIoLockSize = atoi(fileIoLockSize);
                    }
                    m_rwMutex = std::vector<std::shared_mutex>(m_fileIoLockSize);
                }
                else {
                    m_fileIoUseLock = false;
                }
            }
            else {
                m_fileIoUseLock = false;
            }
            const char* fileIoUseCache = getenv(kFileIoUseCache);
            if (fileIoUseCache) {
                if (strcmp(fileIoUseCache, "True") == 0) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "FileIO: Using cache\n");
                    m_fileIoUseCache = true;
                }
                else {
                    m_fileIoUseCache = false;
                }
            }
            if (m_fileIoUseCache) {
                const char* fileIoCachePageNum = getenv(kFileIoCachePageNum);
                if(fileIoCachePageNum) {
                    m_pWriteCache = new LRUCache(atoi(fileIoCachePageNum));
                } else {
                    m_pWriteCache = new LRUCache(kSsdFileIoDefaultCachePageNum);
                }
            }

            if (recovery) {
                m_mappingPath += "_blockmapping";
                Load(m_mappingPath, blockSize, capacity);
            } else if(fileexists(m_mappingPath.c_str())) {
                Load(m_mappingPath, blockSize, capacity);
            } else {
                m_pBlockMapping.Initialize(0, 1, blockSize, capacity);
            }
            for (int i = 0; i < bufferSize; i++) {
                m_buffer.push((uintptr_t)(new AddressType[m_blockLimit]));
            }
            m_compactionThreadPool = std::make_shared<Helper::ThreadPool>();
            m_compactionThreadPool->init(compactionThreads);
            if (recovery) {
                if (m_pBlockController.Recovery(std::string(filePath), batchSize) != ErrorCode::Success) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Fail to Recover FileIO!\n");
                    exit(0);
                }
            } else if (!m_pBlockController.Initialize(batchSize)) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Fail to Initialize FileIO!\n");
                exit(0);
            }
            m_shutdownCalled = false;
        }

        ~FileIO() {
            ShutDown();
        }

        void ShutDown() override {
            if (m_shutdownCalled) {
                return;
            }
            if (!m_mappingPath.empty()) Save(m_mappingPath);
            // TODO: 这里是不是应该加锁？
            for (int i = 0; i < m_pBlockMapping.R(); i++) {
                if (At(i) != 0xffffffffffffffff) delete[]((AddressType*)At(i));
            }
            while (!m_buffer.empty()) {
                uintptr_t ptr;
                if (m_buffer.try_pop(ptr)) delete[]((AddressType*)ptr);
            }
            m_pBlockController.ShutDown();
            if (m_fileIoUseCache) {
                delete m_pWriteCache;
            }
            m_shutdownCalled = true;
        }

        inline uintptr_t& At(SizeType key) {
            return *(m_pBlockMapping[key]);
        }

        ErrorCode Get(SizeType key, ByteArray& value) {
            auto get_begin_time = std::chrono::high_resolution_clock::now();
            if (m_fileIoUseLock) {
                m_rwMutex[hash(key)].lock_shared();
            }
            SizeType r;
            if (m_fileIoUseLock) {
                m_updateMutex.lock_shared();
                r = m_pBlockMapping.R();
                m_updateMutex.unlock_shared();
            }
            else {
                r = m_pBlockMapping.R();
            }
            if (key >= r) return ErrorCode::Fail;
            
            if (m_fileIoUseCache) {
                auto size = ((AddressType*)At(key))[0];
                std::uint8_t* outdata = new std::uint8_t[size];
                if (m_pWriteCache->get(key, outdata, size)) {
                    value.Set(outdata, size, false);
                    if (m_fileIoUseLock) {
                        m_rwMutex[hash(key)].unlock_shared();
                    }
                    auto get_end_time = std::chrono::high_resolution_clock::now();
                    get_time_vec[id] += std::chrono::duration_cast<std::chrono::microseconds>(get_end_time - get_begin_time).count();
                    return ErrorCode::Success;
                }
                delete[] outdata;
            }

            // if (m_pBlockController.ReadBlocks((AddressType*)At(key), value)) {
            //     return ErrorCode::Success;
            // }
            auto begin_time = std::chrono::high_resolution_clock::now();
            auto result = m_pBlockController.ReadBlocks((AddressType*)At(key), value);
            auto end_time = std::chrono::high_resolution_clock::now();
            read_time_vec[id] += std::chrono::duration_cast<std::chrono::microseconds>(end_time - begin_time).count();
            get_times_vec[id]++;
            if (m_fileIoUseLock) {
                m_rwMutex[hash(key)].unlock_shared();
            }
            auto get_end_time = std::chrono::high_resolution_clock::now();
            get_time_vec[id] += std::chrono::duration_cast<std::chrono::microseconds>(get_end_time - get_begin_time).count();
            return result ? ErrorCode::Success : ErrorCode::Fail;
        }

        ErrorCode Get(SizeType key, std::string* value) override {
            auto get_begin_time = std::chrono::high_resolution_clock::now();
            if (m_fileIoUseLock) {
                m_rwMutex[hash(key)].lock_shared();
            }
            SizeType r;
            if (m_fileIoUseLock) {
                m_updateMutex.lock_shared();
                r = m_pBlockMapping.R();
                m_updateMutex.unlock_shared();
            }
            else {
                r = m_pBlockMapping.R();
            }
            if (key >= r) return ErrorCode::Fail;

            if (m_fileIoUseCache) {
                auto size = ((AddressType*)At(key))[0];
                value->resize(size);
                if (m_pWriteCache->get(key, value->data(), size)) {
                    if (m_fileIoUseLock) {
                        m_rwMutex[hash(key)].unlock_shared();
                    }
                    auto get_end_time = std::chrono::high_resolution_clock::now();
                    get_time_vec[id] += std::chrono::duration_cast<std::chrono::microseconds>(get_end_time - get_begin_time).count();
                    return ErrorCode::Success;
                }
            }   
            
            // if (m_pBlockController.ReadBlocks((AddressType*)At(key), value)) {
            //     return ErrorCode::Success;
            // }
            auto begin_time = std::chrono::high_resolution_clock::now();
            auto result = m_pBlockController.ReadBlocks((AddressType*)At(key), value);
            auto end_time = std::chrono::high_resolution_clock::now();
            read_time_vec[id] += std::chrono::duration_cast<std::chrono::microseconds>(end_time - begin_time).count();
            get_times_vec[id]++;
            if (m_fileIoUseLock) {
                m_rwMutex[hash(key)].unlock_shared();
            }
            auto get_end_time = std::chrono::high_resolution_clock::now();
            get_time_vec[id] += std::chrono::duration_cast<std::chrono::microseconds>(get_end_time - get_begin_time).count();
            return result ? ErrorCode::Success : ErrorCode::Fail;
        }

        ErrorCode Get(SizeType key, std::string* value, const std::chrono::microseconds &timeout) {
            auto get_begin_time = std::chrono::high_resolution_clock::now();
            if (m_fileIoUseLock) {
                m_rwMutex[hash(key)].lock_shared();
            }
            SizeType r;
            if (m_fileIoUseLock) {
                m_updateMutex.lock_shared();
                r = m_pBlockMapping.R();
                m_updateMutex.unlock_shared();
            }
            else {
                r = m_pBlockMapping.R();
            }
            if (key >= r) return ErrorCode::Fail;

            if (m_fileIoUseCache) {
                auto size = ((AddressType*)At(key))[0];
                value->resize(size);
                if (m_pWriteCache->get(key, value->data(), size)) {
                    if (m_fileIoUseLock) {
                        m_rwMutex[hash(key)].unlock_shared();
                    }
                    auto get_end_time = std::chrono::high_resolution_clock::now();
                    get_time_vec[id] += std::chrono::duration_cast<std::chrono::microseconds>(get_end_time - get_begin_time).count();
                    return ErrorCode::Success;
                }
            }
            
            
            // if (m_pBlockController.ReadBlocks((AddressType*)At(key), value)) {
            //     return ErrorCode::Success;
            // }
            auto begin_time = std::chrono::high_resolution_clock::now();
            auto result = m_pBlockController.ReadBlocks((AddressType*)At(key), value, timeout);
            auto end_time = std::chrono::high_resolution_clock::now();
            read_time_vec[id] += std::chrono::duration_cast<std::chrono::microseconds>(end_time - begin_time).count();
            get_times_vec[id]++;
            if (m_fileIoUseLock) {
                m_rwMutex[hash(key)].unlock_shared();
            }
            auto get_end_time = std::chrono::high_resolution_clock::now();
            get_time_vec[id] += std::chrono::duration_cast<std::chrono::microseconds>(get_end_time - get_begin_time).count();
            return result ? ErrorCode::Success : ErrorCode::Fail;
        }

        ErrorCode Get(const std::string& key, std::string* value) override {
            return Get(std::stoi(key), value);
        }

        ErrorCode MultiGet(const std::vector<SizeType>& keys, std::vector<std::string>* values, const std::chrono::microseconds &timeout = std::chrono::microseconds::max()) {
            std::vector<AddressType*> blocks;
            std::set<int> lock_keys;
            if (m_fileIoUseLock) {
                // 这里要去重？
                // for (SizeType key : keys) {
                //     lock_keys.insert(hash(key));
                // }
                for (SizeType key : keys) {
                    m_rwMutex[hash(key)].lock_shared();
                }
            }
            SizeType r;
            values->resize(keys.size());
            int i = 0;
            for (SizeType key : keys) {
                if (m_fileIoUseLock) {
                    m_updateMutex.lock_shared();
                    r = m_pBlockMapping.R();
                    m_updateMutex.unlock_shared();
                }
                else {
                    r = m_pBlockMapping.R();
                }
                if (key < r) {
                    if (m_fileIoUseCache) {
                        auto size = ((AddressType*)At(key))[0];
                        (*values)[i].resize(size);
                        if (m_pWriteCache->get(key, (*values)[i].data(), size)) {
                            blocks.push_back(nullptr);
                        }
                        else {
                            blocks.push_back((AddressType*)At(key));
                        }
                    } else {
                        blocks.push_back((AddressType*)At(key));
                    }
                    
                }
                else {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Fail to read key:%d total key number:%d\n", key, r);
                }
                i++;
            }
            // if (m_pBlockController.ReadBlocks(blocks, values, timeout)) return ErrorCode::Success;
            auto result = m_pBlockController.ReadBlocks(blocks, values, timeout);
            if (m_fileIoUseLock) {
                for (SizeType key : keys) {
                    m_rwMutex[hash(key)].unlock_shared();
                }
            }
            return result ? ErrorCode::Success : ErrorCode::Fail;
        }

        ErrorCode MultiGet(const std::vector<std::string>& keys, std::vector<std::string>* values, const std::chrono::microseconds &timeout = std::chrono::microseconds::max()) override {
            std::vector<SizeType> int_keys;
            for (const auto& key : keys) {
                int_keys.push_back(std::stoi(key));
            }
            return MultiGet(int_keys, values, timeout);
        }

        ErrorCode Scan(const SizeType start_key, const int record_count, std::vector<ByteArray> &values, const std::chrono::microseconds &timeout = std::chrono::microseconds::max()) {
            std::vector<SizeType> keys;
            std::vector<AddressType*> blocks;
            SizeType curr_key = start_key;
            while(keys.size() < record_count && curr_key < m_pBlockMapping.R()) {
                if (m_fileIoUseLock) {
                    m_rwMutex[hash(curr_key)].lock_shared();
                }
                if (At(curr_key) == 0xffffffffffffffff) {
                    if (m_fileIoUseLock) {
                        m_rwMutex[hash(curr_key)].unlock_shared();
                    }
                    curr_key++;
                    continue;
                }
                keys.push_back(curr_key);
                blocks.push_back((AddressType*)At(curr_key));
                curr_key++;
            }
            auto result = m_pBlockController.ReadBlocks(blocks, values, timeout);
            if (m_fileIoUseLock) {
                for (auto key : keys) {
                    m_rwMutex[hash(key)].unlock_shared();
                }
            }
            return result ? ErrorCode::Success : ErrorCode::Fail;
        }

        ErrorCode Put(SizeType key, const std::string& value) override {
            int blocks = ((value.size() + PageSize - 1) >> PageSizeEx);
            if (blocks >= m_blockLimit) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Fail to put key:%d value:%lld since value too long!\n", key, value.size());
                return ErrorCode::Fail;
            }
            // 计算是否需要更多的Mapping块
            if (m_fileIoUseLock) {
                m_rwMutex[hash(key)].lock();
            }
            int delta;
            if (m_fileIoUseLock) {
                m_updateMutex.lock_shared();
                delta = key + 1 - m_pBlockMapping.R();
                m_updateMutex.unlock_shared();
            }
            else {
                delta = key + 1 - m_pBlockMapping.R();
            }
            if (delta > 0) {
                    // std::lock_guard<std::mutex> lock(m_updateMutex);
                m_updateMutex.lock();
                delta = key + 1 - m_pBlockMapping.R();
                if (delta > 0) {
                    m_pBlockMapping.AddBatch(delta);
                }
                m_updateMutex.unlock();
            }

            if (m_fileIoUseCache) {
                m_pWriteCache->put(key, (void *)(value.data()), value.size());
            }

            // 如果这个key还没有分配过Mapping块，就分配一组
            if (At(key) == 0xffffffffffffffff) {
                // m_buffer里有多的块就直接用，没有就new一组
                if (m_buffer.unsafe_size() > m_bufferLimit) {
                    uintptr_t tmpblocks;
                    while (!m_buffer.try_pop(tmpblocks));
                    At(key) = tmpblocks;
                }
                else {
                    At(key) = (uintptr_t)(new AddressType[m_blockLimit]);
                }
                // 块地址列表里的0号元素代表数据大小，将其设为-1
                memset((AddressType*)At(key), -1, sizeof(AddressType) * m_blockLimit);
            }
            int64_t* postingSize = (int64_t*)At(key);
            // postingSize小于0说明是新分配的Mapping块，直接获取磁盘块，写入数据
            if (*postingSize < 0) {
                m_pBlockController.GetBlocks(postingSize + 1, blocks);
                m_pBlockController.WriteBlocks(postingSize + 1, blocks, value);
                *postingSize = value.size();
            }
            else {
                uintptr_t tmpblocks;
                // 从buffer里拿一组Mapping块，一会再还一组回去
                while (!m_buffer.try_pop(tmpblocks));
                // 获取一组新的磁盘块，直接写入数据
                // 为保证Checkpoint的效果，这里必须分配新的块进行写入
                m_pBlockController.GetBlocks((AddressType*)tmpblocks + 1, blocks);
                m_pBlockController.WriteBlocks((AddressType*)tmpblocks + 1, blocks, value);
                *((int64_t*)tmpblocks) = value.size();

                // 释放原有的块
                m_pBlockController.ReleaseBlocks(postingSize + 1, (*postingSize + PageSize -1) >> PageSizeEx);
                At(key) = tmpblocks;
                m_buffer.push((uintptr_t)postingSize);
            }
            if (m_fileIoUseLock) {
                m_rwMutex[hash(key)].unlock();
            }
            return ErrorCode::Success;
        }

        ErrorCode Put(SizeType key, const ByteArray& value) {
            int blocks = ((value.Length() + PageSize - 1) >> PageSizeEx);
            if (blocks >= m_blockLimit) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Fail to put key:%d value:%lld since value too long!\n", key, value.Length());
                return ErrorCode::Fail;
            }
            // 计算是否需要更多的Mapping块
            if (m_fileIoUseLock) {
                m_rwMutex[hash(key)].lock();
            }
            int delta;
            if (m_fileIoUseLock) {
                m_updateMutex.lock_shared();
                delta = key + 1 - m_pBlockMapping.R();
                m_updateMutex.unlock_shared();
            }
            else {
                delta = key + 1 - m_pBlockMapping.R();
            }
            if (delta > 0) {
                    // std::lock_guard<std::mutex> lock(m_updateMutex);
                m_updateMutex.lock();
                delta = key + 1 - m_pBlockMapping.R();
                if (delta > 0) {
                    m_pBlockMapping.AddBatch(delta);
                }
                m_updateMutex.unlock();
            }

            if (m_fileIoUseCache) {
                m_pWriteCache->put(key, (void *)(value.Data()), value.Length());
            }

            // 如果这个key还没有分配过Mapping块，就分配一组
            if (At(key) == 0xffffffffffffffff) {
                // m_buffer里有多的块就直接用，没有就new一组
                if (m_buffer.unsafe_size() > m_bufferLimit) {
                    uintptr_t tmpblocks;
                    while (!m_buffer.try_pop(tmpblocks));
                    At(key) = tmpblocks;
                }
                else {
                    At(key) = (uintptr_t)(new AddressType[m_blockLimit]);
                }
                // 块地址列表里的0号元素代表数据大小，将其设为-1
                memset((AddressType*)At(key), -1, sizeof(AddressType) * m_blockLimit);
            }
            int64_t* postingSize = (int64_t*)At(key);
            // postingSize小于0说明是新分配的Mapping块，直接获取磁盘块，写入数据
            if (*postingSize < 0) {
                m_pBlockController.GetBlocks(postingSize + 1, blocks);
                m_pBlockController.WriteBlocks(postingSize + 1, blocks, value);
                *postingSize = value.Length();
            }
            else {
                uintptr_t tmpblocks;
                // 从buffer里拿一组Mapping块，一会再还一组回去
                while (!m_buffer.try_pop(tmpblocks));
                // 获取一组新的磁盘块，直接写入数据
                // 为保证Checkpoint的效果，这里必须分配新的块进行写入
                m_pBlockController.GetBlocks((AddressType*)tmpblocks + 1, blocks);
                m_pBlockController.WriteBlocks((AddressType*)tmpblocks + 1, blocks, value);
                *((int64_t*)tmpblocks) = value.Length();

                // 释放原有的块
                m_pBlockController.ReleaseBlocks(postingSize + 1, (*postingSize + PageSize -1) >> PageSizeEx);
                At(key) = tmpblocks;
                m_buffer.push((uintptr_t)postingSize);
            }
            if (m_fileIoUseLock) {
                m_rwMutex[hash(key)].unlock();
            }
            return ErrorCode::Success;
        }

        ErrorCode Put(const std::string &key, const std::string& value) override {
            return Put(std::stoi(key), value);
        }

        ErrorCode Merge(SizeType key, const std::string& value) {
            if (m_fileIoUseLock) {
                m_rwMutex[hash(key)].lock();
            }
            SizeType r;
            if (m_fileIoUseLock) {
                m_updateMutex.lock_shared();
                r = m_pBlockMapping.R();
                m_updateMutex.unlock_shared();
            }
            else {
                r = m_pBlockMapping.R();
            }
            if (key >= r) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Key range error: key: %d, mapping size: %d\n", key, r);
                if (m_fileIoUseLock) {
                    m_rwMutex[hash(key)].unlock();
                }
                return ErrorCode::Fail;
            }
            
            int64_t* postingSize = (int64_t*)At(key);

            if (m_fileIoUseCache) {
                m_pWriteCache->merge(key, (void *)(value.data()), value.size(), *postingSize);
            }

            auto newSize = *postingSize + value.size();
            int newblocks = ((newSize + PageSize - 1) >> PageSizeEx);
            if (newblocks >= m_blockLimit) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failt to merge key:%d value:%lld since value too long!\n", key, newSize);
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Origin Size: %lld, merge size: %lld\n", *postingSize, value.size());
                if (m_fileIoUseLock) {
                    m_rwMutex[hash(key)].unlock();
                }
                return ErrorCode::Fail;
            }

            auto sizeInPage = (*postingSize) % PageSize;    // 最后一个块的实际大小
            int oldblocks = (*postingSize >> PageSizeEx);
            int allocblocks = newblocks - oldblocks;
            // 最后一个块没有写满的话，需要先读出来，然后拼接新的数据，再写回去
            if (sizeInPage != 0) {
                std::string newValue;
                AddressType readreq[] = { sizeInPage, *(postingSize + 1 + oldblocks) };
                m_pBlockController.ReadBlocks(readreq, &newValue);
                newValue += value;

                uintptr_t tmpblocks;
                while (!m_buffer.try_pop(tmpblocks));
                memcpy((AddressType*)tmpblocks, postingSize, sizeof(AddressType) * (oldblocks + 1));
                m_pBlockController.GetBlocks((AddressType*)tmpblocks + 1 + oldblocks, allocblocks);
                m_pBlockController.WriteBlocks((AddressType*)tmpblocks + 1 + oldblocks, allocblocks, newValue);
                *((int64_t*)tmpblocks) = newSize;

                // 这里也是为了保证Checkpoint，所以将原本没用满的块释放，分配一个新的
                m_pBlockController.ReleaseBlocks(postingSize + 1 + oldblocks, 1);
                At(key) = tmpblocks;
                m_buffer.push((uintptr_t)postingSize);
            }
            else {  // 否则直接分配一组块接在后面
                m_pBlockController.GetBlocks(postingSize + 1 + oldblocks, allocblocks);
                m_pBlockController.WriteBlocks(postingSize + 1 + oldblocks, allocblocks, value);
                *postingSize = newSize;
            }
            if (m_fileIoUseLock) {
                m_rwMutex[hash(key)].unlock();
            }
            return ErrorCode::Success;
        }

        ErrorCode Merge(const std::string &key, const std::string& value) {
            return Merge(std::stoi(key), value);
        }

        ErrorCode Delete(SizeType key) override {
            if (m_fileIoUseLock) {
                m_rwMutex[hash(key)].lock();
            }
            SizeType r;
            if (m_fileIoUseLock) {
                m_updateMutex.lock_shared();
                r = m_pBlockMapping.R();
                m_updateMutex.unlock_shared();
            }
            else {
                r = m_pBlockMapping.R();
            }
            if (key >= r) return ErrorCode::Fail;

            if (m_fileIoUseCache) {
                m_pWriteCache->del(key);
            }

            int64_t* postingSize = (int64_t*)At(key);
            if (*postingSize < 0) {
                if (m_fileIoUseLock) {
                    m_rwMutex[hash(key)].unlock();
                }
                return ErrorCode::Fail;
            }

            int blocks = ((*postingSize + PageSize - 1) >> PageSizeEx);
            m_pBlockController.ReleaseBlocks(postingSize + 1, blocks);
            m_buffer.push((uintptr_t)postingSize);
            At(key) = 0xffffffffffffffff;
            if (m_fileIoUseLock) {
                m_rwMutex[hash(key)].unlock();
            }
            return ErrorCode::Success;
        }

        ErrorCode Delete(const std::string &key) {
            return Delete(std::stoi(key));
        }

        void ForceCompaction() {
            Save(m_mappingPath);
        }

        void GetStat() {
            int remainBlocks = m_pBlockController.RemainBlocks();
            int remainGB = (long long)remainBlocks << PageSizeEx >> 30;
            // int remainGB = remainBlocks >> 20 << 2;
            SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Remain %d blocks, totally %d GB\n", remainBlocks, remainGB);
            uint64_t get_times = 0;
            uint64_t get_time = 0;
            uint64_t read_time = 0;
            for (int i = 0; i < get_time_vec.size(); i++) {
                get_times += get_times_vec[i];
                get_time += get_time_vec[i];
                read_time += read_time_vec[i];
            }
            double average_read_time = (double)read_time / get_times;
            double average_get_time = (double)get_time / get_times;
            SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Get times: %llu, get time: %llu us, read time: %llu us\n", get_times, get_time, read_time);
            SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Average read time: %lf us, average get time: %lf us\n", average_read_time, average_get_time);
            m_pBlockController.IOStatistics();
        }

        ErrorCode Load(std::string path, SizeType blockSize, SizeType capacity) {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Load mapping From %s\n", path.c_str());
            auto ptr = f_createIO();
            if (ptr == nullptr || !ptr->Initialize(path.c_str(), std::ios::binary | std::ios::in)) return ErrorCode::FailedOpenFile;

            SizeType CR, mycols;
            IOBINARY(ptr, ReadBinary, sizeof(SizeType), (char*)&CR);
            IOBINARY(ptr, ReadBinary, sizeof(SizeType), (char*)&mycols);
            if (mycols > m_blockLimit) m_blockLimit = mycols;

            m_pBlockMapping.Initialize(CR, 1, blockSize, capacity);
            for (int i = 0; i < CR; i++) {
                At(i) = (uintptr_t)(new AddressType[m_blockLimit]);
                IOBINARY(ptr, ReadBinary, sizeof(AddressType) * mycols, (char*)At(i));
            }
            SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Load mapping (%d,%d) Finish!\n", CR, mycols);
            return ErrorCode::Success;
        }
        
        ErrorCode Save(std::string path) {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Save mapping To %s\n", path.c_str());
            auto ptr = f_createIO();
            if (ptr == nullptr || !ptr->Initialize(path.c_str(), std::ios::binary | std::ios::out)) return ErrorCode::FailedCreateFile;

            SizeType CR = m_pBlockMapping.R();
            IOBINARY(ptr, WriteBinary, sizeof(SizeType), (char*)&CR);
            IOBINARY(ptr, WriteBinary, sizeof(SizeType), (char*)&m_blockLimit);
            std::vector<AddressType> empty(m_blockLimit, 0xffffffffffffffff);
            for (int i = 0; i < CR; i++) {
                if (At(i) == 0xffffffffffffffff) {
                    IOBINARY(ptr, WriteBinary, sizeof(AddressType) * m_blockLimit, (char*)(empty.data()));
                }
                else {
                    int64_t* postingSize = (int64_t*)At(i);
                    IOBINARY(ptr, WriteBinary, sizeof(AddressType) * m_blockLimit, (char*)postingSize);
                }
            }
            SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Save mapping (%d,%d) Finish!\n", CR, m_blockLimit);
            return ErrorCode::Success;
        }

        bool Initialize(bool debug = false) override {
            if (debug) SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Initialize FileIO for new threads\n");
            m_freeIdMutex.lock();
            if (m_freeId.empty()) {
                id = m_maxId++;
            }
            else {
                id = m_freeId.front();
                m_freeId.pop();
            }
            while(read_time_vec.size() < m_maxId) read_time_vec.push_back(0);
            while(get_time_vec.size() < m_maxId) get_time_vec.push_back(0);
            while(get_times_vec.size() < m_maxId) get_times_vec.push_back(0);
            m_freeIdMutex.unlock();
            return m_pBlockController.Initialize(64);
        }

        bool ExitBlockController(bool debug = false) override { 
            if (debug) SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Exit FileIO for thread\n");
            m_freeIdMutex.lock();
            m_freeId.push(id);
            m_freeIdMutex.unlock();
            return m_pBlockController.ShutDown(); 
        }

        ErrorCode Checkpoint(std::string prefix) override {
            std::string filename = prefix + "_blockmapping";
            SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "FileIO: saving block mapping\n");
            Save(filename);
            return m_pBlockController.Checkpoint(prefix);
        }

    private:
        static constexpr const char* kFileIoUseLock = "SPFRESH_FILE_IO_USE_LOCK";
        static constexpr bool kFileIoDefaultUseLock = false;
        static constexpr const char* kFileIoLockSize = "SPFRESH_FILE_IO_LOCK_SIZE";
        static constexpr int kFileIoDefaultLockSize = 1024;
        static constexpr const char* kFileIoUseCache = "SPFRESH_FILE_IO_USE_CACHE";
        static constexpr bool kFileIoDefaultUseCache = false;
        static constexpr const char* kFileIoCachePageNum = "SPFRESH_FILE_IO_CACHE_PAGE_NUM";
        static constexpr int kSsdFileIoDefaultCachePageNum = 2048;

        static thread_local int id;
        int m_maxId = 0;
        std::queue<int> m_freeId;
        std::mutex m_freeIdMutex;
        std::vector<uint64_t> read_time_vec;
        std::vector<uint64_t> get_time_vec;
        std::vector<uint64_t> get_times_vec;

        bool m_fileIoUseLock = kFileIoDefaultUseLock;
        int m_fileIoLockSize = kFileIoDefaultLockSize;
        bool m_fileIoUseCache = kFileIoDefaultUseCache;
        std::string m_mappingPath;
        SizeType m_blockLimit;
        COMMON::Dataset<uintptr_t> m_pBlockMapping;
        SizeType m_bufferLimit;
        tbb::concurrent_queue<uintptr_t> m_buffer;

        std::shared_ptr<Helper::ThreadPool> m_compactionThreadPool;
        BlockController m_pBlockController;
        LRUCache *m_pWriteCache;

        bool m_shutdownCalled;
        std::shared_mutex m_updateMutex;
        std::vector<std::shared_mutex> m_rwMutex;

        inline int hash(int key) {
            return key % m_fileIoLockSize;
        }
    };
}
#endif