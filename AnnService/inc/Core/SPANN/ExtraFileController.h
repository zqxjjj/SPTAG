#ifndef _SPTAG_SPANN_EXTRAFILECONTROLLER_H_
#define _SPTAG_SPANN_EXTRAFILECONTROLLER_H_
#include "inc/Helper/KeyValueIO.h"
#include "inc/Core/Common/Dataset.h"
#include "inc/Core/VectorIndex.h"
#include "inc/Helper/ThreadPool.h"
#include <cstdlib>
#include <memory>
#include <atomic>
#include <mutex>
#include <fcntl.h>
#include <tbb/concurrent_queue.h>
#include <tbb/concurrent_hash_map.h>
namespace SPTAG::SPANN {
    typedef std::int64_t AddressType;
    class FileIO : public Helper::KeyValueIO {
        class BlockController {
        private:
            #ifndef USE_HELPER_THREADPOOL
            class ThreadPool {
            public:
                ThreadPool(size_t numThreads, int fd, BlockController* ctrl);
                ~ThreadPool();

                void notify_one();
            private:
                std::vector<pthread_t> workers;
                BlockController* ctrl;
                std::mutex queueMutex;
                std::condition_variable condition;
                int fd;
                bool stop;

                static void* workerThread(void* arg);
                void threadLoop();
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

            tbb::concurrent_queue<AddressType> m_blockAddresses;
            tbb::concurrent_queue<AddressType> m_blockAddresses_reserve;
            
            pthread_t m_fileIoTid;
            volatile bool m_fileIoThreadStartFailed = false;
            volatile bool m_fileIoThreadReady = false;
            volatile bool m_fileIoThreadExiting = false;

            int m_ssdFileIoAlignment = kSsdFileIoDefaultAlignment;
            int m_ssdFileIoDepth = kSsdFileIoDefaultIoDepth;
            int m_ssdFileIoThreadNum = kSsdFileIoDefaultIoThreadNum;
            struct SubIoRequest {
                tbb::concurrent_queue<SubIoRequest *>* completed_sub_io_requests;
                void* app_buff;
                void* io_buff;
                bool is_read;
                AddressType real_size;
                AddressType offset;
                BlockController* ctrl;
                int posting_id;
            };
            tbb::concurrent_queue<SubIoRequest *> m_submittedSubIoRequests;
            struct IoContext {
                std::vector<SubIoRequest> sub_io_requests;
                std::vector<SubIoRequest *> free_sub_io_requests;
                tbb::concurrent_queue<SubIoRequest *> completed_sub_io_requests;
                int in_flight = 0;
            };
            static thread_local struct IoContext m_currIoContext;

            #ifndef USE_HELPER_THREADPOOL
            ThreadPool* m_threadPool;
            #endif

            static int m_ssdInflight;

            static std::unique_ptr<char[]> m_memBuffer;

            std::mutex m_initMutex;
            int m_numInitCalled = 0;

            int m_batchSize;
            static int m_ioCompleteCount;
            int m_preIOCompleteCount = 0;
            std::chrono::time_point<std::chrono::high_resolution_clock> m_preTime = std::chrono::high_resolution_clock::now();

            static void* InitializeFileIo(void* args);

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

            int RemainBlocks() {
                return m_blockAddresses.unsafe_size();
            };

            ErrorCode Checkpoint(std::string prefix) {
                std::string filename = prefix + "_blockpool";
                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "SPDK: saving block pool\n");
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
                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "SPDK Recovery: Loading block pool\n");
                std::string filename = prefix + "_blockpool";
                auto ptr = f_createIO();
                if (ptr == nullptr || !ptr->Initialize(filename.c_str(), std::ios::binary | std::ios::in)) {
                    return ErrorCode::FailedCreateFile;
                }
                int blocks;
                IOBINARY(ptr, ReadBinary, sizeof(SizeType), (char*)&blocks);
                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "SPDK Recovery: Reading %d blocks to pool\n", blocks);
                AddressType currBlockAddress = 0;
                for (int i = 0; i < blocks; i++) {
                    IOBINARY(ptr, ReadBinary, sizeof(AddressType), (char*)&(currBlockAddress));
                    m_blockAddresses.push(currBlockAddress);
                }

                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "SPDK Recovery: Initializing SPDK\n");
                
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
                    sr.completed_sub_io_requests = &(m_currIoContext.completed_sub_io_requests);
                    sr.app_buff = nullptr;
                    sr.io_buff = aligned_alloc(m_ssdFileIoAlignment, PageSize);
                    sr.ctrl = this;
                    m_currIoContext.free_sub_io_requests.push_back(&sr);
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
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Fail to Recover SPDK!\n");
                    exit(0);
                }
            } else if (!m_pBlockController.Initialize(batchSize)) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Fail to Initialize SPDK!\n");
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
            if (key >= m_pBlockMapping.R()) return ErrorCode::Fail;

            if (m_pBlockController.ReadBlocks((AddressType*)At(key), value)) return ErrorCode::Success;
            return ErrorCode::Fail;
        }

        ErrorCode MultiGet(const std::vector<SizeType>& keys, std::vector<std::string>* values, const std::chrono::microseconds &timeout = std::chrono::microseconds::max()) {
            std::vector<AddressType*> blocks;
            for (SizeType key : keys) {
                if (key < m_pBlockMapping.R()) blocks.push_back((AddressType*)At(key));
                else {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Fail to read key:%d total key number:%d\n", key, m_pBlockMapping.R());
                }
            }
            if (m_pBlockController.ReadBlocks(blocks, values, timeout)) return ErrorCode::Success;
            return ErrorCode::Fail; 
        }

        ErrorCode Put(SizeType key, const std::string& value) override {
            int blocks = ((value.size() + PageSize - 1) >> PageSizeEx);
            if (blocks >= m_blockLimit) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failt to put key:%d value:%lld since value too long!\n", key, value.size());
                return ErrorCode::Fail;
            }
            // 计算是否需要更多的Mapping块
            int delta = key + 1 - m_pBlockMapping.R();
            if (delta > 0) {
                {
                    std::lock_guard<std::mutex> lock(m_updateMutex);
                    m_pBlockMapping.AddBatch(delta);
                }
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
                // 这里是否应该加锁？否则可能同一组磁盘块会被多次释放
                m_pBlockController.ReleaseBlocks(postingSize + 1, (*postingSize + PageSize -1) >> PageSizeEx);
                while (InterlockedCompareExchange(&At(key), tmpblocks, (uintptr_t)postingSize) != (uintptr_t)postingSize) {
                    postingSize = (int64_t*)At(key);
                }
                m_buffer.push((uintptr_t)postingSize);
            }
            return ErrorCode::Success;
        }

        ErrorCode Merge(SizeType key, const std::string& value) {
            if (key >= m_pBlockMapping.R()) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Key range error: key: %d, mapping size: %d\n", key, m_pBlockMapping.R());
                return ErrorCode::Fail;
            }

            int64_t* postingSize = (int64_t*)At(key);
            auto newSize = *postingSize + value.size();
            int newblocks = ((newSize + PageSize - 1) >> PageSizeEx);
            if (newblocks >= m_blockLimit) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failt to merge key:%d value:%lld since value too long!\n", key, newSize);
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Origin Size: %lld, merge size: %lld\n", *postingSize, value.size());
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
                while (InterlockedCompareExchange(&At(key), tmpblocks, (uintptr_t)postingSize) != (uintptr_t)postingSize) {
                    postingSize = (int64_t*)At(key);
                }
                m_buffer.push((uintptr_t)postingSize);
            }
            else {  // 否则直接分配一组块接在后面
                m_pBlockController.GetBlocks(postingSize + 1 + oldblocks, allocblocks);
                m_pBlockController.WriteBlocks(postingSize + 1 + oldblocks, allocblocks, value);
                *postingSize = newSize;
            }
            return ErrorCode::Success;
        }

        ErrorCode Delete(SizeType key) override {
            if (key >= m_pBlockMapping.R()) return ErrorCode::Fail;
            int64_t* postingSize = (int64_t*)At(key);
            if (*postingSize < 0) return ErrorCode::Fail;

            int blocks = ((*postingSize + PageSize - 1) >> PageSizeEx);
            m_pBlockController.ReleaseBlocks(postingSize + 1, blocks);
            m_buffer.push((uintptr_t)postingSize);
            At(key) = 0xffffffffffffffff;
            return ErrorCode::Success;
        }

        void ForceCompaction() {
            Save(m_mappingPath);
        }

        void GetStat() {
            int remainBlocks = m_pBlockController.RemainBlocks();
            int remainGB = remainBlocks << PageSizeEx >> 30;
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
            if (debug) SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Initialize SPDK for new threads\n");
            return m_pBlockController.Initialize(64);
        }

        bool ExitBlockController(bool debug = false) override { 
            if (debug) SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Exit SPDK for thread\n");
            return m_pBlockController.ShutDown(); 
        }

        ErrorCode Checkpoint(std::string prefix) override {
            std::string filename = prefix + "_blockmapping";
            SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "SPDK: saving block mapping\n");
            Save(filename);
            return m_pBlockController.Checkpoint(prefix);
        }

    private:
        std::string m_mappingPath;
        SizeType m_blockLimit;
        COMMON::Dataset<uintptr_t> m_pBlockMapping;
        SizeType m_bufferLimit;
        tbb::concurrent_queue<uintptr_t> m_buffer;

        std::shared_ptr<Helper::ThreadPool> m_compactionThreadPool;
        BlockController m_pBlockController;

        bool m_shutdownCalled;
        std::mutex m_updateMutex;
    };
}
#endif