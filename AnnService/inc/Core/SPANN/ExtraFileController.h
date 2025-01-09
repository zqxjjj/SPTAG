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
namespace SPTAG::SPANN {
    typedef std::int64_t AddressType;
    class FileIO : public Helper::KeyValueIO {
        class BlockController {
        private:
            class WriteCache {
            public:
                WriteCache(int pageSize, int pageNum) {
                    this->pageSize = pageSize;
                    this->pageNum = pageNum;
                    m_memCache.reserve(pageNum);
                    for (int i = 0; i < pageNum; i++) {
                        m_memCache.push_back(std::unique_ptr<char[]>(new char[pageSize]));
                        m_memCacheFree.push(i);
                    }
                    m_cacheMutex = std::vector<std::shared_mutex>(pageNum);
                }
                ~WriteCache() {
                    m_memCache.clear();
                }
                bool AddPages(AddressType* p_data, int p_size, const std::string& p_value) {
                    // SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "AddPages: %d\n", p_size);
                    if (p_size > pageNum || p_size > m_memCacheFree.unsafe_size()) return false;
                    m_popMutex.lock();
                    std::vector<int> cachePages(p_size);
                    bool success = true;
                    int max_pos = 0;
                    for(int i = 0; i < p_size; i++) {
                        if (!m_memCacheFree.try_pop(cachePages[i])) {
                            success = false;
                            max_pos = i;
                            break;
                        }
                    }
                    m_popMutex.unlock();
                    if (!success) {
                        for (int i = 0; i < max_pos; i++) {
                            m_memCacheFree.push(cachePages[i]);
                        }
                        // SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "AddPages: %d failed\n", p_size);
                        return false;
                    }
                    int totalSize = p_value.size();
                    for (int i = 0; i < p_size; i++) {
                        // m_cacheMutex[cachePages[i]].lock();
                        memcpy(m_memCache[cachePages[i]].get(), (void*)p_value.data() + i * PageSize, (PageSize * (i + 1)) > totalSize ? (totalSize - i * PageSize) : PageSize);
                        m_memCacheMap.insert(std::make_pair(p_data[i], cachePages[i]));
                        // m_cacheMutex[cachePages[i]].unlock();
                    }
                    // SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "AddPages: %d success\n", p_size);
                    return true;
                }
                bool GetPage(AddressType p_data, void *p_value, AddressType real_size) {
                    // SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "GetPage: %d\n", p_data);
                    if (real_size > PageSize) {
                        // SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "GetPage: %d failed, real_size > PageSize!\n", p_data);
                        return false;
                    }
                    if (real_size <= 0) {
                        // SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "GetPage: %d failed, real_size <= 0!\n", p_data);
                        return false;
                    }
                    int cachePage;
                    tbb::concurrent_hash_map<AddressType, int>::accessor cachePageAccessor;
                    if (!m_memCacheMap.find(cachePageAccessor, p_data)) {
                        // SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "GetPage: %d failed\n", p_data);
                        return false;
                    }
                    cachePage = cachePageAccessor->second;
                    // m_cacheMutex[cachePage].lock_shared();
                    // if (m_memCacheMap.find(cachePageAccessor, p_data) && cachePageAccessor->second == cachePage) { // 这个==其实可以去掉
                    //     memcpy(p_value, m_memCache[cachePage].get(), PageSize);
                    //     m_cacheMutex[cachePage].unlock_shared();
                    //     return true;
                    // }
                    // else {
                    //     m_cacheMutex[cachePage].unlock_shared();
                    //     return false;
                    // }
                    auto tmpPage = std::make_unique<char[]>(PageSize);
                    memcpy(tmpPage.get(), m_memCache[cachePage].get(), real_size);
                    if (m_memCacheMap.find(cachePageAccessor, p_data) && cachePageAccessor->second == cachePage) {
                        memcpy(p_value, tmpPage.get(), real_size);
                        // SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "GetPage: %d success\n", p_data);
                        return true;
                    }
                    else {
                        // SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "GetPage: %d failed\n", p_data);
                        return false;
                    }
                }
                bool removePage(AddressType p_data) {
                    // SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "removePage: %d\n", p_data);
                    tbb::concurrent_hash_map<AddressType, int>::accessor cachePageAccessor;
                    if (!m_memCacheMap.find(cachePageAccessor, p_data)) {
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "removePage: %ld failed\n", p_data);
                        return false;
                    }
                    // SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Successfully find page %d\n", p_data);
                    int cachePage = cachePageAccessor->second;
                    // m_cacheMutex[cachePage].lock();
                    // if (m_memCacheMap.find(cachePageAccessor, p_data) && cachePageAccessor->second == cachePage) {
                    //     m_memCacheMap.erase(cachePageAccessor);
                    //     m_memCacheFree.push(cachePage);
                    //     m_cacheMutex[cachePage].unlock();
                    //     return true;
                    // }
                    // else {
                    //     m_cacheMutex[cachePage].unlock();
                    //     return false;
                    // }
                    auto result = m_memCacheMap.erase(cachePageAccessor);
                    if (result) {
                        m_memCacheFree.push(cachePage);
                        // SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "removePage: %d success\n", p_data);
                        return true;
                    }
                    else {
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "removePage: %ld failed2\n", p_data);
                        return false;
                    }
                    // SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "removePage: %d %s\n", p_data, result ? "success" : "failed");
                    // return result;
                }
                bool RemainWriteJobs() {
                    return m_memCacheMap.size() > 0;
                }
                int getCacheSize() {
                    return m_memCacheMap.size();
                }
                void getMapStat() {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Cache Map Status:\n", m_memCacheMap.size());
                    for (auto it = m_memCacheMap.begin(); it != m_memCacheMap.end(); it++) {
                        printf("Key: %ld Value: %d\n", it->first, it->second);
                    }
                }
            private:
                int pageSize;
                int pageNum;
                std::vector<std::unique_ptr<char[]>> m_memCache;
                tbb::concurrent_hash_map<AddressType, int> m_memCacheMap;
                tbb::concurrent_queue<int> m_memCacheFree;
                std::mutex m_popMutex;
                std::vector<std::shared_mutex> m_cacheMutex;
            };
            #ifndef USE_HELPER_THREADPOOL
            class ThreadPool {
            public:
                ThreadPool(size_t numThreads, int fd, BlockController* ctrl, BlockController::WriteCache* wc);
                struct ThreadArgs {
                    size_t id;
                    ThreadPool* pool;
                };
                ~ThreadPool();

                void notify_one();
                int get_read_count() {
                    int count = 0;
                    for (int i = 0; i < read_complete_vec.size(); i++) {
                        count += read_complete_vec[i];
                    }
                    return count;
                };
                int get_write_count() {
                    int count = 0;
                    for (int i = 0; i < write_complete_vec.size(); i++) {
                        count += write_complete_vec[i];
                    }
                    return count;
                };
                int64_t get_busy_time() {
                    int64_t time = 0;
                    for (int i = 0; i < busy_time_vec.size(); i++) {
                        time += busy_time_vec[i];
                    }
                    return time;
                };
                int64_t get_io_time() {
                    int64_t time = 0;
                    for (int i = 0; i < io_time_vec.size(); i++) {
                        time += io_time_vec[i];
                    }
                    return time;
                };
                int64_t get_remove_page_time() {
                    int64_t time = 0;
                    for (int i = 0; i < remove_page_time_vec.size(); i++) {
                        time += remove_page_time_vec[i];
                    }
                    return time;
                };
                int get_busy_thread_num() {
                    int count = 0;
                    for (int i = 0; i < busy_thread_vec.size(); i++) {
                        if (busy_thread_vec[i]) {
                            count++;
                        }
                    }
                    return count;
                };

                std::pair<int, int> get_notify_times() {
                    int count = 0;
                    for (int i = 0; i < notify_times_vec.size(); i++) {
                        count += notify_times_vec[i];
                    }
                    return std::make_pair(notify_times, count);
                }
            private:
                std::vector<pthread_t> workers;
                BlockController* ctrl;
                std::mutex queueMutex;
                std::condition_variable condition;
                std::atomic<bool> stop;
                std::vector<ThreadArgs> threadArgs_vec;
                std::vector<int64_t> busy_time_vec;
                std::vector<int64_t> io_time_vec;
                std::vector<int> read_complete_vec;
                std::vector<int> write_complete_vec;
                std::vector<int> remove_page_time_vec;
                std::vector<int> notify_times_vec;
                std::vector<bool> busy_thread_vec;
                BlockController::WriteCache* m_writeCache;
                int fd;
                int notify_times;
                // bool stop;

                static void* workerThread(void* arg);
                void threadLoop(size_t id);
            };
            friend ThreadPool;
            #endif
            
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
            static constexpr const char* kFileIoCachePageNum = "SPFRESH_FILE_IO_CACHE_PAGE_NUM";
            static constexpr int kSsdFileIoDefaultCachePageNum = 512;

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
                #ifndef USE_ASYNC_IO
                tbb::concurrent_queue<SubIoRequest *>* completed_sub_io_requests;
                tbb::concurrent_queue<SubIoRequest *>* free_sub_io_requests;
                #endif
                void* app_buff;
                void* io_buff;
                bool is_read;
                bool need_free;
                AddressType real_size;
                AddressType offset;
                BlockController* ctrl;
                int posting_id;
            };
            tbb::concurrent_queue<SubIoRequest *> m_submittedSubIoRequests;
            struct IoContext {
                #ifdef USE_ASYNC_IO
                std::vector<SubIoRequest> sub_io_requests;
                std::queue<SubIoRequest *> free_sub_io_requests;
                int in_flight = 0;
                #else
                std::vector<SubIoRequest> sub_io_requests;
                tbb::concurrent_queue<SubIoRequest *> free_sub_io_requests;
                tbb::concurrent_queue<SubIoRequest *> completed_sub_io_requests;
                int in_flight = 0;
                #endif
            };
            static thread_local struct IoContext m_currIoContext;
            static thread_local int debug_fd;
            #ifdef USE_ASYNC_IO
            static thread_local uint64_t iocp;
            #endif
            static std::chrono::high_resolution_clock::time_point m_startTime;

            #ifndef USE_HELPER_THREADPOOL
            ThreadPool* m_threadPool = nullptr;
            #endif

            #ifdef USE_ASYNC_IO
            static thread_local int id;
            int m_maxId = 0;
            std::queue<int> m_idQueue;
            std::vector<int> read_complete_vec;
            std::vector<int> write_complete_vec;
            std::vector<int> write_submit_vec;
            #endif

            WriteCache* m_writeCache = nullptr;

            std::mutex m_uniqueResourceMutex;

            static int m_ssdInflight;

            static std::unique_ptr<char[]> m_memBuffer;

            std::mutex m_initMutex;
            int m_numInitCalled = 0;

            int m_batchSize;
            static std::atomic<int> m_ioCompleteCount;
            int m_preIOCompleteCount = 0;
            std::chrono::time_point<std::chrono::high_resolution_clock> m_preTime = std::chrono::high_resolution_clock::now();

            static void* InitializeFileIo(void* args);

            static void* IoStatisticsThread(void* args) {
                auto ctrl = static_cast<BlockController*>(args);
                #ifndef USE_ASYNC_IO
                while (!ctrl->m_fileIoThreadExiting) {
                    auto working_thread_num = ctrl->m_threadPool->get_busy_thread_num();
                    auto remain_tasks_num = ctrl->m_submittedSubIoRequests.unsafe_size();
                    auto notify_times = ctrl->m_threadPool->get_notify_times();
                    auto free_io_jobs = ctrl->m_currIoContext.free_sub_io_requests.unsafe_size();
                    auto write_jobs_in_cache = ctrl->m_writeCache->getCacheSize();
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "FileIO: working thread num: %d, remain tasks num: %d, notify called: %d, notify times: %d, write jobs in cache: %d\n", working_thread_num, remain_tasks_num, notify_times.first, notify_times.second, write_jobs_in_cache);
                    std::this_thread::sleep_for(std::chrono::seconds(5));
                }
                #endif
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

            bool ReadBlocks(const std::vector<AddressType*>& p_data, std::vector<std::string>* p_value, const std::chrono::microseconds &timeout = std::chrono::microseconds::max());

            bool WriteBlocks(AddressType* p_data, int p_size, const std::string& p_value);

            bool IOStatistics();

            bool ShutDown();

            bool RemainWriteJobs() {
                return m_writeCache->RemainWriteJobs();
            }

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
                    #ifndef USE_ASYNC_IO
                    sr.completed_sub_io_requests = &(m_currIoContext.completed_sub_io_requests);
                    sr.free_sub_io_requests = &(m_currIoContext.free_sub_io_requests);
                    #endif
                    sr.app_buff = nullptr;
                    sr.io_buff = aligned_alloc(m_ssdFileIoAlignment, PageSize);
                    if (sr.io_buff == nullptr) {
                        fprintf(stderr, "FileIO::BlockController::Initialize failed: aligned_alloc failed\n");
                        return ErrorCode::Fail;
                    }
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

    public:
        FileIO(const char* filePath, SizeType blockSize, SizeType capacity, SizeType postingBlocks, SizeType bufferSize = 1024, int batchSize = 64, bool recovery = false, int compactionThreads = 1) {
            // TODO: 后面还得再看看，可能需要修改
            m_mappingPath = std::string(filePath);
            m_blockLimit = postingBlocks + 1;
            m_bufferLimit = bufferSize;

            const char* fileIoUseLock = getenv(kFileIoUseLock);
            if(fileIoUseLock) {
                if(strcmp(fileIoUseLock, "True") == 0) {
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
            m_shutdownCalled = true;
        }

        inline uintptr_t& At(SizeType key) {
            return *(m_pBlockMapping[key]);
        }

        ErrorCode Get(SizeType key, std::string* value) override {
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
            
            // if (m_pBlockController.ReadBlocks((AddressType*)At(key), value)) {
            //     return ErrorCode::Success;
            // }
            auto result = m_pBlockController.ReadBlocks((AddressType*)At(key), value);
            if (m_fileIoUseLock) {
                m_rwMutex[hash(key)].unlock_shared();
            }
            return result ? ErrorCode::Success : ErrorCode::Fail;
        }

        ErrorCode MultiGet(const std::vector<SizeType>& keys, std::vector<std::string>* values, const std::chrono::microseconds &timeout = std::chrono::microseconds::max()) {
            std::vector<AddressType*> blocks;
            if (m_fileIoUseLock) {
                // TODO: 这里是不是需要考虑哈希冲突的问题？感觉上不需要
                for (SizeType key : keys) {
                    m_rwMutex[hash(key)].lock_shared();
                }
            }
            SizeType r;
            for (SizeType key : keys) {
                if (m_fileIoUseLock) {
                    m_updateMutex.lock_shared();
                    r = m_pBlockMapping.R();
                    m_updateMutex.unlock_shared();
                }
                else {
                    r = m_pBlockMapping.R();
                }
                if (key < r) blocks.push_back((AddressType*)At(key));
                else {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Fail to read key:%d total key number:%d\n", key, r);
                }
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

        void ForceCompaction() {
            Save(m_mappingPath);
        }

        void GetStat() {
            int remainBlocks = m_pBlockController.RemainBlocks();
            int remainGB = (long long)remainBlocks << PageSizeEx >> 30;
            // int remainGB = remainBlocks >> 20 << 2;
            SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Remain %d blocks, totally %d GB\n", remainBlocks, remainGB);
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
            if (m_pBlockController.RemainWriteJobs()) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "There are still write jobs in progress, wait for them to finish\n");
            }
            while (m_pBlockController.RemainWriteJobs());
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
            return m_pBlockController.Initialize(64);
        }

        bool ExitBlockController(bool debug = false) override { 
            if (debug) SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Exit FileIO for thread\n");
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

        bool m_fileIoUseLock = kFileIoDefaultUseLock;
        int m_fileIoLockSize = kFileIoDefaultLockSize;
        std::string m_mappingPath;
        SizeType m_blockLimit;
        COMMON::Dataset<uintptr_t> m_pBlockMapping;
        SizeType m_bufferLimit;
        tbb::concurrent_queue<uintptr_t> m_buffer;

        std::shared_ptr<Helper::ThreadPool> m_compactionThreadPool;
        BlockController m_pBlockController;

        bool m_shutdownCalled;
        std::shared_mutex m_updateMutex;
        std::vector<std::shared_mutex> m_rwMutex;

        inline int hash(int key) {
            return key % m_fileIoLockSize;
        }
    };
}
#endif