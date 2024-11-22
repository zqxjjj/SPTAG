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

            static void FileIoLoop(void *arg);

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
        };

    public:
        FileIO(const char* filePath, SizeType blockSize, SizeType capacity, SizeType postingBlocks, SizeType bufferSize = 1024, int batchSize = 64, bool recovery = false, int compactionThreads = 1) {
            // TODO: 后面还得再看看，可能需要修改
            m_mappingPath = std::string(filePath);
            m_blockLimit = postingBlocks + 1;
            m_bufferLimit = bufferSize;
            if (recovery) {
                // TODO
            } else if(fileexists(m_mappingPath.c_str())) {
                // TODO
            } else {
                m_pBlockMapping.Initialize(0, 1, blockSize, capacity);
            }
            for (int i = 0; i < bufferSize; i++) {
                m_buffer.push((uintptr_t)(new AddressType[m_blockLimit]));
            }
            m_compactionThreadPool = std::make_shared<Helper::ThreadPool>();
            m_compactionThreadPool->init(compactionThreads);
            if (recovery) {
                // TODO
            } else if (!m_pBlockController.Initialize(batchSize)) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Fail to Initialize SPDK!\n");
                exit(0);
            }
            m_shutdownCalled = false;
        }

    private:
        std::string m_mappingPath;
        SizeType m_blockLimit;
        COMMON::Dataset<AddressType> m_pBlockMapping;
        SizeType m_bufferLimit;
        tbb::concurrent_queue<AddressType> m_buffer;

        std::shared_ptr<Helper::ThreadPool> m_compactionThreadPool;
        BlockController m_pBlockController;

        bool m_shutdownCalled;
        std::mutex m_updateMutex;
    };
}
#endif