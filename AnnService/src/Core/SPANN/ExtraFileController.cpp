#include "inc/Core/SPANN/ExtraFileController.h"

namespace SPTAG::SPANN
{
thread_local struct FileIO::BlockController::IoContext FileIO::BlockController::m_currIoContext;
thread_local int FileIO::BlockController::debug_fd = -1;
#ifdef USE_ASYNC_IO
thread_local uint64_t FileIO::BlockController::iocp = 0;
thread_local int FileIO::BlockController::id = 0;
#endif
std::chrono::high_resolution_clock::time_point FileIO::BlockController::m_startTime;
int FileIO::BlockController::m_ssdInflight = 0;
std::atomic<int> FileIO::BlockController::m_ioCompleteCount(0);
int FileIO::BlockController::fd = -1;
char* FileIO::BlockController::filePath = new char[1024];
std::unique_ptr<char[]> FileIO::BlockController::m_memBuffer;
#ifndef USE_HELPER_THREADPOOL

FileIO::BlockController::ThreadPool::ThreadPool(size_t numThreads, int fd, BlockController* ctrl, BlockController::WriteCache* wc) {
#ifdef USE_ASYNC_IO

#else
    this->fd = fd;
    this->ctrl = ctrl;
    this->m_writeCache = wc;
    this->notify_times = 0;
    stop = false;
    busy_time_vec.resize(numThreads);
    io_time_vec.resize(numThreads);
    read_complete_vec.resize(numThreads);
    write_complete_vec.resize(numThreads);
    remove_page_time_vec.resize(numThreads);
    notify_times_vec.resize(numThreads);
    busy_thread_vec.resize(numThreads);
    for (size_t i = 0; i < numThreads; i++) {
        workers.push_back(pthread_t());
        threadArgs_vec.push_back(ThreadArgs{i, this});
        pthread_create(&workers.back(), nullptr, workerThread, &threadArgs_vec.back());
        busy_time_vec[i] = 0;
        io_time_vec[i] = 0;
        read_complete_vec[i] = 0;
        write_complete_vec[i] = 0;
        remove_page_time_vec[i] = 0;
        notify_times_vec[i] = 0;
        busy_thread_vec[i] = false;
    }
    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "FileIO::BlockController::ThreadPool initialized with %d threads, fd=%d\n", numThreads, fd);
#endif
}

FileIO::BlockController::ThreadPool::~ThreadPool() {
    stop = true;
    condition.notify_all();
    for (auto& worker : workers) {
        pthread_join(worker, nullptr);
    }
}

void FileIO::BlockController::ThreadPool::notify_one() {
    notify_times++;
    condition.notify_one();
}

void* FileIO::BlockController::ThreadPool::workerThread(void* arg) {
    auto args = static_cast<ThreadArgs*>(arg);
    auto pool = args->pool;
    pool->threadLoop(args->id);
    return nullptr;
}

void FileIO::BlockController::ThreadPool::threadLoop(size_t id) {
#ifdef USE_ASYNC_IO

#else
    while(true) {
        {
            std::unique_lock<std::mutex> lock(queueMutex);
            condition.wait(lock, [this] { return stop || !ctrl->m_submittedSubIoRequests.empty();});
        }
        notify_times_vec[id]++;
        busy_thread_vec[id] = true;
        auto start_time = std::chrono::high_resolution_clock::now();
        SubIoRequest* currSubIo = nullptr;
        while(ctrl->m_submittedSubIoRequests.try_pop(currSubIo)) {
            // ctrl->m_ssdInflight++;
            if(currSubIo->is_read) {
                // fprintf(stderr, "pread\n");
                auto io_begin_time = std::chrono::high_resolution_clock::now();
                ssize_t bytesRead = pread(fd, currSubIo->io_buff, PageSize, currSubIo->offset);
                auto io_end_time = std::chrono::high_resolution_clock::now();
                auto io_time = std::chrono::duration_cast<std::chrono::microseconds>(io_end_time - io_begin_time).count();
                io_time_vec[id] += io_time;
                if(bytesRead == -1) {
                    auto err_str = strerror(errno);
                    fprintf(stderr, "pread failed: %s\n", err_str);
                    stop = true;
                }
                else {
                    read_complete_vec[id]++;
                }
                currSubIo->completed_sub_io_requests->push(currSubIo);
            }
            else {
                // fprintf(stderr, "pwrite\n");
                // SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "FileIO::BlockController::ThreadPool::threadLoop: write page %ld\n", currSubIo->offset / PageSize);
                auto io_begin_time = std::chrono::high_resolution_clock::now();
                ssize_t bytesWritten = pwrite(fd, currSubIo->io_buff, PageSize, currSubIo->offset);
                // SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "FileIO::BlockController::ThreadPool::threadLoop: write page %ld done\n", currSubIo->offset / PageSize);
                auto io_end_time = std::chrono::high_resolution_clock::now();
                auto io_time = std::chrono::duration_cast<std::chrono::microseconds>(io_end_time - io_begin_time).count();
                io_time_vec[id] += io_time;
                if(bytesWritten == -1) {
                    auto err_str = strerror(errno);
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "pwrite failed: %s, Block offset: %d, io buf address: %p\n", err_str, currSubIo->offset, currSubIo->io_buff);
                    stop = true;
                }
                else {
                    write_complete_vec[id]++;
                }
                if (currSubIo->need_free) {
                    currSubIo->free_sub_io_requests->push(currSubIo);
                    if (currSubIo->offset % PageSize != 0) {
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "FileIO::BlockController::ThreadPool::threadLoop: write page %ld not aligned\n", currSubIo->offset / PageSize);
                    }
                    auto remove_page_begin_time = std::chrono::high_resolution_clock::now();
                    auto result = m_writeCache->removePage(currSubIo->offset / PageSize);
                    if (result == false) {
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "FileIO::BlockController::ThreadPool::threadLoop: write cache remove page %ld failed\n", currSubIo->offset / PageSize);
                    }
                    auto remove_page_end_time = std::chrono::high_resolution_clock::now();
                    auto remove_page_time = std::chrono::duration_cast<std::chrono::microseconds>(remove_page_end_time - remove_page_begin_time).count();
                    remove_page_time_vec[id] += remove_page_time;
                    // printf("FileIO::BlockController::ThreadPool::threadLoop: write cache remove page %ld\n", currSubIo->offset / PageSize);
                }
                else {
                    // printf("FileIO::BlockController::ThreadPool::threadLoop: does not remove page %ld\n", currSubIo->offset / PageSize);
                    currSubIo->completed_sub_io_requests->push(currSubIo);
                }
            }
        }
        if(stop) {
            break;
        }
        auto end_time = std::chrono::high_resolution_clock::now();
        auto busy_time = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
        busy_time_vec[id] += busy_time;
        busy_thread_vec[id] = false;
    }
#endif
}

#endif

void* FileIO::BlockController::InitializeFileIo(void* args) {
    FileIO::BlockController* ctrl = (FileIO::BlockController *)args;
    struct stat st;
    const char* fileIoPath = getenv(kFileIoPath);
    auto fileSize = kSsdImplMaxNumBlocks << PageSizeEx;
    if(fileIoPath) {
        strcpy(filePath, fileIoPath);
    }
    else {
        fprintf(stderr, "FileIO::BlockController::InitializeFileIo failed: No filePath\n");
        ctrl->m_fileIoThreadStartFailed = true;
        fd = -1;
        // strcpy(filePath, "/home/lml/SPFreshTest/testfile");
    }
    if(stat(filePath, &st) != 0) {
        fd = open(filePath, O_CREAT | O_WRONLY, 0666);
        if(fd == -1) {
            fprintf(stderr, "FileIO::BlockController::InitializeFileIo failed\n");
            // return nullptr;
            ctrl->m_fileIoThreadStartFailed = true;
        }
        else {
            if (fallocate(fd, 0, 0, fileSize) == -1) {
                fprintf(stderr, "FileIO::BlockController::InitializeFileIo failed: fallocate failed\n");
                ctrl->m_fileIoThreadStartFailed = true;
                fd = -1;
            } else {
                close(fd);
                fd = open(filePath, O_RDWR | O_DIRECT);
                if (fd == -1) {
                    auto err_str = strerror(errno);
                    fprintf(stderr, "open failed: %s\n", err_str);
                    ctrl->m_fileIoThreadStartFailed = true;
                } else {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "FileIO::BlockController::InitializeFileIo: file %s created, fd=%d\n", filePath, fd);
                }
            }
        }
    }
    else {
        auto actualFileSize = st.st_blocks * st.st_blksize;
        if(actualFileSize < fileSize) {
            fd = open(filePath, O_CREAT | O_WRONLY, 0666);
            if(fd == -1) {
                fprintf(stderr, "FileIO::BlockController::InitializeFileIo failed\n");
                // return nullptr;
                ctrl->m_fileIoThreadStartFailed = true;
            }
            else {
                if (fallocate(fd, 0, 0, fileSize) == -1) {
                    fprintf(stderr, "FileIO::BlockController::InitializeFileIo failed: fallocate failed\n");
                    ctrl->m_fileIoThreadStartFailed = true;
                    fd = -1;
                } else {
                    close(fd);
                    fd = open(filePath, O_RDWR | O_DIRECT);
                    if (fd == -1) {
                        auto err_str = strerror(errno);
                        fprintf(stderr, "open failed: %s\n", err_str);
                        ctrl->m_fileIoThreadStartFailed = true;
                    } else {
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "FileIO::BlockController::InitializeFileIo: file %s created, fd=%d\n", filePath, fd);
                    }
                }
            }
        } else {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "FileIO::BlockController::InitializeFileIo: file has been created with enough space.\n", filePath, fd);
            fd = open(filePath, O_RDWR | O_DIRECT);
            if (fd == -1) {
                auto err_str = strerror(errno);
                fprintf(stderr, "open failed: %s\n", err_str);
                ctrl->m_fileIoThreadStartFailed = true;
            } else {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "FileIO::BlockController::InitializeFileIo: file %s opened, fd=%d\n", filePath, fd);
            }
        }
    }
    const char* fileIoDepth = getenv(kFileIoDepth);
    if (fileIoDepth) ctrl->m_ssdFileIoDepth = atoi(fileIoDepth);
    const char* fileIoThreadNum = getenv(kFileIoThreadNum);
    if (fileIoThreadNum) ctrl->m_ssdFileIoThreadNum = atoi(fileIoThreadNum);
    const char* fileIoAlignment = getenv(kFileIoAlignment);
    if (fileIoAlignment) ctrl->m_ssdFileIoAlignment = atoi(fileIoAlignment);

    if(ctrl->m_fileIoThreadStartFailed == false) {
        ctrl->m_fileIoThreadReady = true;
        // std::lock_guard<std::mutex> lock(ctrl->m_uniqueResourceMutex);
        #ifndef USE_ASYNC_IO
        const char* fileIoCachePageNum = getenv(kFileIoCachePageNum);
        if (fileIoCachePageNum) {
            ctrl->m_writeCache = new WriteCache(PageSize, atoi(fileIoCachePageNum));
        }
        else {
            ctrl->m_writeCache = new WriteCache(PageSize, kSsdFileIoDefaultCachePageNum);
        }
        ctrl->m_threadPool = new ThreadPool(ctrl->m_ssdFileIoThreadNum, fd, ctrl, ctrl->m_writeCache);
        pthread_create(&ctrl->m_ioStatisticsTid, NULL, &IoStatisticsThread, ctrl);
        #endif
        // m_ssdInflight = 0;
    }
    pthread_exit(NULL);
}

bool FileIO::BlockController::Initialize(int batchSize) {
    std::lock_guard<std::mutex> lock(m_initMutex);
    m_numInitCalled++;

    if(m_numInitCalled == 1) {
        m_batchSize = batchSize;
        m_startTime = std::chrono::high_resolution_clock::now();
        for(AddressType i = 0; i < kSsdImplMaxNumBlocks; i++) {
            m_blockAddresses.push(i);
        }
        pthread_create(&m_fileIoTid, NULL, &InitializeFileIo, this);
        while(!m_fileIoThreadReady && !m_fileIoThreadStartFailed);
        if(m_fileIoThreadStartFailed) {
            fprintf(stderr, "FileIO::BlockController::Initialize failed\n");
            return false;
        }
    }
    if (m_idQueue.empty()) {
        id = m_maxId;
        m_maxId++;
    }
    else {
        id = m_idQueue.front();
        m_idQueue.pop();
    }
    while(read_complete_vec.size() <= id) {
        read_complete_vec.push_back(0);
    }
    while(write_complete_vec.size() <= id) {
        write_complete_vec.push_back(0);
    }
    while(write_submit_vec.size() <= id) {
        write_submit_vec.push_back(0);
    }
    m_currIoContext.sub_io_requests.resize(m_ssdFileIoDepth);
    m_currIoContext.in_flight = 0;
    for(auto &sr : m_currIoContext.sub_io_requests) {
        #ifndef USE_ASYNC_IO
        sr.completed_sub_io_requests = &(m_currIoContext.completed_sub_io_requests);
        sr.free_sub_io_requests = &(m_currIoContext.free_sub_io_requests);
        #endif
        sr.app_buff = nullptr;
        sr.io_buff = aligned_alloc(m_ssdFileIoAlignment, PageSize);
        if (sr.io_buff == nullptr) {
            fprintf(stderr, "FileIO::BlockController::Initialize failed: aligned_alloc failed\n");
            return false;
        }
        sr.ctrl = this;
        m_currIoContext.free_sub_io_requests.push(&sr);
    }
    auto ret = syscall(__NR_io_setup, m_ssdFileIoDepth, &iocp);
    if (ret < 0) {
        fprintf(stderr, "FileIO::BlockController::Initialize failed: io_setup failed\n");
        return false;
    }
#ifdef USE_FILE_DEBUG
    auto debug_file_name = std::string("/nvme1n1/lml/") + std::to_string(m_numInitCalled) + "_debug.log";
    debug_fd = open(debug_file_name.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0666);
    if (debug_fd == -1) {
        fprintf(stderr, "FileIO::BlockController::Initialize failed: open debug file failed\n");
        return false;
    }
#endif
    return true;
}

bool FileIO::BlockController::GetBlocks(AddressType* p_data, int p_size) {
    AddressType currBlockAddress = 0;
#ifdef USE_FILE_DEBUG
    auto debug_string = std::to_string(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now() - m_startTime).count()) + " 1";
    auto result = pwrite(debug_fd, debug_string.c_str(), debug_string.size(), 0);
    if (result == -1) {
        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "FileIO::BlockController::GetBlocks: %s\n", strerror(errno));
    }
    fsync(debug_fd);
#endif
    for(int i = 0; i < p_size; i++) {
        while(!m_blockAddresses.try_pop(currBlockAddress));
        p_data[i] = currBlockAddress;
    }
    return true;
}

bool FileIO::BlockController::ReleaseBlocks(AddressType* p_data, int p_size) {
#ifdef USE_FILE_DEBUG
    auto debug_string = std::to_string(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now() - m_startTime).count()) + " 2";
    auto result = pwrite(debug_fd, debug_string.c_str(), debug_string.size(), 0);
    if (result == -1) {
        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "FileIO::BlockController::ReleaseBlocks: pwrite failed\n");
    }
    fsync(debug_fd);
#endif
    for(int i = 0; i < p_size; i++) {
        m_blockAddresses_reserve.push(p_data[i]);
    }
    return true;
}

bool FileIO::BlockController::ReadBlocks(AddressType* p_data, std::string* p_value, const std::chrono::microseconds &timeout) {
#ifdef USE_FILE_DEBUG
    auto debug_string = std::to_string(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now() - m_startTime).count()) + " 3";
    auto result = pwrite(debug_fd, debug_string.c_str(), debug_string.size(), 0);
    if (result == -1) {
        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "FileIO::BlockController::ReadBlocks: pwrite failed\n");
    }
    fsync(debug_fd);
#endif
    #ifdef USE_ASYNC_IO
    p_value->resize(p_data[0]);
    AddressType currOffset = 0;
    AddressType dataIdx = 1;
    auto blockNum = (p_data[0] + PageSize - 1) >> PageSizeEx;
    if (blockNum > m_ssdFileIoDepth) {
        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "FileIO::BlockController::ReadBlocks: blockNum > m_ssdFileIoDepth\n");
        return false;
    }
    if (blockNum > m_currIoContext.free_sub_io_requests.size()) {
        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "FileIO::BlockController::ReadBlocks: blockNum > m_currIoContext.free_sub_io_requests.size()\n");
        return false;
    }
    std::vector<struct iocb> myiocbs(blockNum);
    std::vector<struct iocb*> iocbs(blockNum);
    memset(myiocbs.data(), 0, sizeof(struct iocb) * blockNum);
    int submitted = 0, done = 0;

    for (int i = 0; i < blockNum; i++) {
        auto currSubIo = m_currIoContext.free_sub_io_requests.front();
        m_currIoContext.free_sub_io_requests.pop();
        currSubIo->app_buff = (void*)p_value->data() + currOffset;
        currSubIo->real_size = (p_data[0] - currOffset) < PageSize ? (p_data[0] - currOffset) : PageSize;
        currSubIo->is_read = true;
        currSubIo->offset = p_data[dataIdx] * PageSize;
        myiocbs[i].aio_fildes = fd;
        myiocbs[i].aio_lio_opcode = IOCB_CMD_PREAD;
        myiocbs[i].aio_buf = (uint64_t)currSubIo->io_buff;
        myiocbs[i].aio_nbytes = (p_data[0] - currOffset) < PageSize ? (p_data[0] - currOffset) : PageSize;
        myiocbs[i].aio_offset = p_data[dataIdx] * PageSize;
        myiocbs[i].aio_data = reinterpret_cast<uintptr_t>(currSubIo);
        iocbs[i] = &myiocbs[i];
        currOffset += PageSize;
        dataIdx++;
    }
    std::vector<struct io_event> events(blockNum);
    int totalDone = 0, totalSubmitted = 0;
    struct timespec timeout_ts {0, timeout.count() * 1000};
    auto t1 = std::chrono::high_resolution_clock::now();
    while(totalDone < blockNum) {
        auto t2 = std::chrono::high_resolution_clock::now();
        // if(std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1) > timeout) {
        //     SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "FileIO::BlockController::ReadBlocks: timeout\n");
        //     for(int i = totalDone; i < totalSubmitted; i++) {
        //         syscall(__NR_io_cancel, iocp, iocbs[i], &events[i]);
        //         reinterpret_cast<SubIoRequest*>(iocbs[i]->aio_data)->app_buff = nullptr;
        //         m_currIoContext.free_sub_io_requests.push(reinterpret_cast<SubIoRequest*>(iocbs[i]->aio_data));
        //     }
        //     for (int i = totalSubmitted; i < blockNum; i++) {
        //         reinterpret_cast<SubIoRequest*>(iocbs[i]->aio_data)->app_buff = nullptr;
        //         m_currIoContext.free_sub_io_requests.push(reinterpret_cast<SubIoRequest*>(iocbs[i]->aio_data));
        //     }
        //     return false;
        // }
        if(totalSubmitted < blockNum) {
            int s = syscall(__NR_io_submit, iocp, blockNum - totalSubmitted, iocbs.data() + totalSubmitted);
            if(s > 0) {
                totalSubmitted += s;
            }
            else {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "FileIO::BlockController::ReadBlocks: io_submit failed\n");
            }
        }
        int wait = totalSubmitted - totalDone;
        auto d = syscall(__NR_io_getevents, iocp, wait, wait, events.data() + totalDone, &timeout_ts);
        for (int i = totalDone; i < totalDone + d; i++) {
            auto req = reinterpret_cast<SubIoRequest*>(events[i].data);
            memcpy(req->app_buff, req->io_buff, req->real_size);
            req->app_buff = nullptr;
            m_currIoContext.free_sub_io_requests.push(req);
        }
        totalDone += d;
        read_complete_vec[id] += d;
    }
    return true;
    #else
    p_value->resize(p_data[0]);
    AddressType currOffset = 0;
    AddressType dataIdx = 1;
    SubIoRequest* currSubIo;

    // Clear timeout I/Os
    while (m_currIoContext.in_flight) {
        if (m_currIoContext.completed_sub_io_requests.try_pop(currSubIo)) {
            currSubIo->app_buff = nullptr;
            m_currIoContext.free_sub_io_requests.push(currSubIo);
            m_currIoContext.in_flight--;
        }
    }

    auto t1 = std::chrono::high_resolution_clock::now();
    while(currOffset < p_data[0] || m_currIoContext.in_flight) {
        auto t2 = std::chrono::high_resolution_clock::now();
        if(std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1) > timeout) {
            // SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "FileIO::BlockController::ReadBlocks: timeout\n");
            return false;
        }
        // Try read from write cache
        if(currOffset < p_data[0]) {
            auto p_addr = p_data[dataIdx];
            void *p_val = (void*)p_value->data() + currOffset;
            auto real_size = (p_data[0] - currOffset) < PageSize ? (p_data[0] - currOffset) : PageSize;
            if (currOffset + real_size > p_value->size()) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "FileIO::BlockController::ReadBlocks: currOffset + real_size > p_value->size()\n");
            }
            if (m_writeCache->GetPage(p_addr, (void*)p_value->data() + currOffset, (p_data[0] - currOffset) < PageSize ? (p_data[0] - currOffset) : PageSize)) {
                currOffset += PageSize;
                dataIdx++;
                continue;
            }
        }
        // Try submit
        if(currOffset < p_data[0] && m_currIoContext.free_sub_io_requests.try_pop(currSubIo)) {
            currSubIo->app_buff = (void*)p_value->data() + currOffset;
            currSubIo->real_size = (p_data[0] - currOffset) < PageSize ? (p_data[0] - currOffset) : PageSize;
            currSubIo->is_read = true;
            currSubIo->offset = p_data[dataIdx] * PageSize;
            m_submittedSubIoRequests.push(currSubIo);
            m_threadPool->notify_one();
            m_currIoContext.in_flight++;
            currOffset += PageSize;
            dataIdx++;
        }
        // Try complete
        if(m_currIoContext.in_flight && m_currIoContext.completed_sub_io_requests.try_pop(currSubIo)) {
            memcpy(currSubIo->app_buff, currSubIo->io_buff, currSubIo->real_size);
            currSubIo->app_buff = nullptr;
            m_currIoContext.free_sub_io_requests.push(currSubIo);
            m_currIoContext.in_flight--;
        }
    }
    return true;
    #endif
}

bool FileIO::BlockController::ReadBlocks(const std::vector<AddressType*>& p_data, std::vector<std::string>* p_values, const std::chrono::microseconds &timeout) {
#ifdef USE_FILE_DEBUG
    auto debug_string = std::to_string(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now() - m_startTime).count()) + " 4";
    auto result = pwrite(debug_fd, debug_string.c_str(), debug_string.size(), 0);
    if (result == -1) {
        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "FileIO::BlockController::ReadBlocks: pwrite failed\n");
    }
    fsync(debug_fd);
#endif
    #ifdef USE_ASYNC_IO
    auto t1 = std::chrono::high_resolution_clock::now();
    p_values->resize(p_data.size());
    const int batch_size = m_batchSize;
    std::vector<struct iocb> myiocbs(batch_size);
    std::vector<struct iocb*> iocbs(batch_size);
    std::vector<struct io_event> events;
    std::vector<SubIoRequest> subIoRequests;
    std::vector<int> subIoRequestCount(p_data.size(), 0);
    subIoRequests.reserve(256);
    memset(myiocbs.data(), 0, sizeof(struct iocb) * batch_size);
    for(size_t i = 0; i < p_data.size(); i++) {
        AddressType* p_data_i = p_data[i];
        std::string* p_value = &((*p_values)[i]);

        p_value->resize(p_data_i[0]);
        AddressType currOffset = 0;
        AddressType dataIdx = 1;

        while(currOffset < p_data_i[0]) {
            SubIoRequest currSubIo;
            currSubIo.app_buff = (void*)p_value->data() + currOffset;
            currSubIo.real_size = (p_data_i[0] - currOffset) < PageSize ? (p_data_i[0] - currOffset) : PageSize;
            currSubIo.is_read = true;
            currSubIo.offset = p_data_i[dataIdx] * PageSize;
            currSubIo.posting_id = i;
            subIoRequests.push_back(currSubIo);
            subIoRequestCount[i]++;
            currOffset += PageSize;
            dataIdx++;
        }
    }

    struct timespec timeout_ts {0, timeout.count() * 1000};
    
    for (int currSubIoStartId = 0; currSubIoStartId < subIoRequests.size(); currSubIoStartId += batch_size) {
        int currSubIoEndId = (currSubIoStartId + batch_size) > subIoRequests.size() ? subIoRequests.size() : currSubIoStartId + batch_size;
        int currSubIoIdx = currSubIoStartId;
        int totalToSubmit = currSubIoEndId - currSubIoStartId;
        int totalSubmitted = 0, totalDone = 0;
        for (int i = 0; i < totalToSubmit; i++) {
            auto currSubIoIdx = currSubIoStartId + i;
            auto currSubIo = m_currIoContext.free_sub_io_requests.front();
            m_currIoContext.free_sub_io_requests.pop();
            myiocbs[i].aio_fildes = fd;
            myiocbs[i].aio_lio_opcode = IOCB_CMD_PREAD;
            myiocbs[i].aio_buf = (uint64_t)currSubIo->io_buff;
            myiocbs[i].aio_nbytes = subIoRequests[currSubIoIdx].real_size;
            myiocbs[i].aio_offset = subIoRequests[currSubIoIdx].offset;
            myiocbs[i].aio_data = reinterpret_cast<uintptr_t>(currSubIo);
            currSubIo->app_buff = subIoRequests[currSubIoIdx].app_buff;
            currSubIo->real_size = subIoRequests[currSubIoIdx].real_size;
            currSubIo->is_read = true;
            currSubIo->offset = subIoRequests[currSubIoIdx].offset;
            currSubIo->posting_id = subIoRequests[currSubIoIdx].posting_id;
            iocbs[i] = &myiocbs[i];
            currSubIoIdx++;
        }
        while (totalDone < totalToSubmit) {
            auto t2 = std::chrono::high_resolution_clock::now();
            // if(std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1) > timeout) {
            //     SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "FileIO::BlockController::ReadBlocks (batch) : timeout\n");
            //     for(int i = totalDone; i < totalSubmitted; i++) {
            //         syscall(__NR_io_cancel, iocp, iocbs[i], &events[i]);
            //         reinterpret_cast<SubIoRequest*>(iocbs[i]->aio_data)->app_buff = nullptr;
            //         m_currIoContext.free_sub_io_requests.push(reinterpret_cast<SubIoRequest*>(iocbs[i]->aio_data));
            //     }
            //     for (int i = totalSubmitted; i < totalToSubmit; i++) {
            //         reinterpret_cast<SubIoRequest*>(iocbs[i]->aio_data)->app_buff = nullptr;
            //         m_currIoContext.free_sub_io_requests.push(reinterpret_cast<SubIoRequest*>(iocbs[i]->aio_data));
            //     }
            //     return false;
            // }
            if(totalSubmitted < totalToSubmit) {
                int s = syscall(__NR_io_submit, iocp, totalToSubmit - totalSubmitted, iocbs.data() + totalSubmitted);
                if(s > 0) {
                    totalSubmitted += s;
                }
                else {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "FileIO::BlockController::ReadBlocks: io_submit failed\n");
                }
            }
            int wait = totalSubmitted - totalDone;
            auto d = syscall(__NR_io_getevents, iocp, wait, wait, events.data() + totalDone, &timeout_ts);
            for (int i = totalDone; i < totalDone + d; i++) {
                auto req = reinterpret_cast<SubIoRequest*>(events[i].data);
                memcpy(req->app_buff, req->io_buff, req->real_size);
                subIoRequestCount[req->posting_id]--;
                req->app_buff = nullptr;
                m_currIoContext.free_sub_io_requests.push(req);
            }
            totalDone += d;
            read_complete_vec[id] += d;
        }
    }

    for (int i = 0; i < subIoRequestCount.size(); i++) {
        if (subIoRequestCount[i] != 0) {
            (*p_values)[i].clear();
        }
    }
    return true;
    #else
    auto t1 = std::chrono::high_resolution_clock::now();
    p_values->resize(p_data.size());
    std::vector<SubIoRequest> subIoRequests;
    std::vector<int> subIoRequestCount(p_data.size(), 0);
    subIoRequests.reserve(256);
    for(size_t i = 0; i < p_data.size(); i++) {
        AddressType* p_data_i = p_data[i];
        std::string* p_value = &((*p_values)[i]);

        p_value->resize(p_data_i[0]);
        AddressType currOffset = 0;
        AddressType dataIdx = 1;

        while(currOffset < p_data_i[0]) {
            SubIoRequest currSubIo;
            currSubIo.app_buff = (void*)p_value->data() + currOffset;
            currSubIo.real_size = (p_data_i[0] - currOffset) < PageSize ? (p_data_i[0] - currOffset) : PageSize;
            currSubIo.is_read = true;
            currSubIo.offset = p_data_i[dataIdx] * PageSize;
            currSubIo.posting_id = i;
            subIoRequests.push_back(currSubIo);
            subIoRequestCount[i]++;
            currOffset += PageSize;
            dataIdx++;
        }
    }

    // Clear timeout I/Os
    while(m_currIoContext.in_flight) {
        SubIoRequest* currSubIo;
        if(m_currIoContext.completed_sub_io_requests.try_pop(currSubIo)) {
            currSubIo->app_buff = nullptr;
            m_currIoContext.free_sub_io_requests.push(currSubIo);
            m_currIoContext.in_flight--;
        }
    }

    const int batch_size = m_batchSize;
    for(int currSubIoStartId = 0; currSubIoStartId < subIoRequests.size(); currSubIoStartId += batch_size) {
        int currSubIoEndId = (currSubIoStartId + batch_size) > subIoRequests.size() ? subIoRequests.size() : currSubIoStartId + batch_size;
        int currSubIoIdx = currSubIoStartId;
        SubIoRequest* currSubIo;
        while(currSubIoIdx < currSubIoEndId || m_currIoContext.in_flight) {
            auto t2 = std::chrono::high_resolution_clock::now();
            if(std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1) > timeout) {
                // SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "FileIO::BlockController::ReadBlocks (batch) : timeout\n");
                break;
            }
            // Try read from write cache
            if(currSubIoIdx < currSubIoEndId) {
                auto p_addr = subIoRequests[currSubIoIdx].offset / PageSize;
                if(m_writeCache->GetPage(p_addr, subIoRequests[currSubIoIdx].app_buff, subIoRequests[currSubIoIdx].real_size)) {
                    subIoRequestCount[subIoRequests[currSubIoIdx].posting_id]--;
                    currSubIoIdx++;
                    continue;
                }
            }
            // Try submit
            if(currSubIoIdx < currSubIoEndId && m_currIoContext.free_sub_io_requests.try_pop(currSubIo)) {
                currSubIo->app_buff = subIoRequests[currSubIoIdx].app_buff;
                currSubIo->real_size = subIoRequests[currSubIoIdx].real_size;
                currSubIo->is_read = true;
                currSubIo->offset = subIoRequests[currSubIoIdx].offset;
                currSubIo->posting_id = subIoRequests[currSubIoIdx].posting_id;
                m_submittedSubIoRequests.push(currSubIo);
                m_threadPool->notify_one();
                m_currIoContext.in_flight++;
                currSubIoIdx++;
            }
            // Try complete
            if(m_currIoContext.in_flight && m_currIoContext.completed_sub_io_requests.try_pop(currSubIo)) {
                memcpy(currSubIo->app_buff, currSubIo->io_buff, currSubIo->real_size);
                currSubIo->app_buff = nullptr;
                subIoRequestCount[currSubIo->posting_id]--;
                m_currIoContext.free_sub_io_requests.push(currSubIo);
                m_currIoContext.in_flight--;
            }
        }

        auto t2 = std::chrono::high_resolution_clock::now();
        if(std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1) > timeout) {
            break;
        }
    }

    for(int i = 0; i < subIoRequestCount.size(); i++) {
        if(subIoRequestCount[i] != 0) {
            (*p_values)[i].clear();
        }
    }
    return true;
    #endif
}

bool FileIO::BlockController::WriteBlocks(AddressType* p_data, int p_size, const std::string& p_value) {
#ifdef USE_FILE_DEBUG
    auto debug_string = std::to_string(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now() - m_startTime).count()) + " 5";
    auto result = pwrite(debug_fd, debug_string.c_str(), debug_string.size(), 0);
    if (result == -1) {
        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "FileIO::BlockController::WriteBlocks: pwrite failed\n");
    }
    fsync(debug_fd);
#endif
#ifdef USE_ASYNC_IO
    AddressType currBlockIdx = 0;
    int inflight = 0;
    SubIoRequest* currSubIo;
    int totalSize = p_value.size();
    // SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "WriteBlocks: %d\n", p_size);
    // Submit all I/Os
    write_submit_vec[id] += p_size;
    std::vector<struct iocb> myiocbs(p_size);
    std::vector<struct iocb*> iocbs(p_size);
    memset(myiocbs.data(), 0, sizeof(struct iocb) * p_size);
    for (int i = 0; i < p_size; i++) {
        auto currSubIo = m_currIoContext.free_sub_io_requests.front();
        m_currIoContext.free_sub_io_requests.pop();
        currSubIo->app_buff = (void*)p_value.data() + currBlockIdx * PageSize;
        currSubIo->real_size = (PageSize * (currBlockIdx + 1)) > totalSize ? (totalSize - currBlockIdx * PageSize) : PageSize;
        memcpy(currSubIo->io_buff, currSubIo->app_buff, currSubIo->real_size);
        currSubIo->is_read = false;
        currSubIo->offset = p_data[currBlockIdx] * PageSize;
        myiocbs[i].aio_fildes = fd;
        myiocbs[i].aio_lio_opcode = IOCB_CMD_PWRITE;
        myiocbs[i].aio_buf = (uint64_t)currSubIo->io_buff;
        myiocbs[i].aio_nbytes = (PageSize * (currBlockIdx + 1)) > totalSize ? (totalSize - currBlockIdx * PageSize) : PageSize;
        myiocbs[i].aio_offset = p_data[currBlockIdx] * PageSize;
        myiocbs[i].aio_data = reinterpret_cast<uintptr_t>(currSubIo);
        iocbs[i] = &myiocbs[i];
        currBlockIdx++;
    }
    std::vector<struct io_event> events(p_size);
    int totalDone = 0, totalSubmitted = 0;
    while(totalDone < p_size) {
        int s = syscall(__NR_io_submit, iocp, p_size - totalSubmitted, iocbs.data() + totalSubmitted);
        if(s > 0) {
            totalSubmitted += s;
        }
        else {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "FileIO::BlockController::WriteBlocks: io_submit failed\n");
        }
        int wait = totalSubmitted - totalDone;
        auto d = syscall(__NR_io_getevents, iocp, wait, wait, events.data() + totalDone, nullptr);
        for (int i = totalDone; i < totalDone + d; i++) {
            auto req = reinterpret_cast<SubIoRequest*>(events[i].data);
            req->app_buff = nullptr;
            m_currIoContext.free_sub_io_requests.push(req);
        }
        totalDone += d;
        write_complete_vec[id] += d;
    }
    return true;
#else 
    AddressType currBlockIdx = 0;
    int inflight = 0;
    SubIoRequest* currSubIo;
    int totalSize = p_value.size();
    // SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "WriteBlocks: %d\n", p_size);
    // Submit all I/Os
    auto need_free = m_writeCache->AddPages(p_data, p_size, p_value);
    while(currBlockIdx < p_size || (inflight && !need_free)) {
        // Try submit
        if(currBlockIdx < p_size && m_currIoContext.free_sub_io_requests.try_pop(currSubIo)) {
            currSubIo->app_buff = (void*)p_value.data() + currBlockIdx * PageSize;
            currSubIo->real_size = (PageSize * (currBlockIdx + 1)) > totalSize ? (totalSize - currBlockIdx * PageSize) : PageSize;
            currSubIo->is_read = false;
            currSubIo->need_free = need_free;
            currSubIo->offset = p_data[currBlockIdx] * PageSize;
            // if (need_free) {
            //     SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "FileIO::BlockController::WriteBlocks: write cache add page %ld\n", p_data[currBlockIdx]);
            // }
            // currSubIo->free_sub_io_requests = &(m_currIoContext.free_sub_io_requests);
            memcpy(currSubIo->io_buff, currSubIo->app_buff, currSubIo->real_size);
            m_submittedSubIoRequests.push(currSubIo);
            m_threadPool->notify_one();
            currBlockIdx++;
            inflight++;
        }
        // Try complete
        if(!need_free && inflight && m_currIoContext.completed_sub_io_requests.try_pop(currSubIo)) {
            currSubIo->app_buff = nullptr;
            m_currIoContext.free_sub_io_requests.push(currSubIo);
            inflight--;
        }
    }
    return true;
#endif
}

bool FileIO::BlockController::IOStatistics() {
    #ifdef USE_ASYNC_IO
    int currReadCount = 0;
    int currWriteCount = 0;
    int write_submit_count = 0;
    for (int i = 0; i < read_complete_vec.size(); i++) {
        currReadCount += read_complete_vec[i];
    }
    for (int i = 0; i < write_complete_vec.size(); i++) {
        currWriteCount += write_complete_vec[i];
    }
    for (int i = 0; i < write_submit_vec.size(); i++) {
        write_submit_count += write_submit_vec[i];
    }
    #else
    int currReadCount = m_threadPool->get_read_count();
    int currWriteCount = m_threadPool->get_write_count();
    #endif
    
    int currIOCount = currReadCount + currWriteCount;
    int diffIOCount = currIOCount - m_preIOCompleteCount;
    m_preIOCompleteCount = currIOCount;

    auto currTime = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(currTime - m_preTime);
    m_preTime = currTime;

    double currIOPS = (double)diffIOCount * 1000 / duration.count();
    double currBandWidth = (double)diffIOCount * PageSize / 1024 * 1000 / 1024 * 1000 / duration.count();

    #ifndef USE_ASYNC_IO
    auto busy_time = m_threadPool->get_busy_time();
    auto io_time = m_threadPool->get_io_time();
    auto remove_page_time = m_threadPool->get_remove_page_time() / 1000;
    auto busy_thread_num = m_threadPool->get_busy_thread_num();
    #endif

    std::cout << "Diff IO Count: " << diffIOCount << " Time: " << duration.count() << "us" << std::endl;
    std::cout << "IOPS: " << currIOPS << "k Bandwidth: " << currBandWidth << "MB/s" << std::endl;
    std::cout << "Read Count: " << currReadCount << " Write Count: " << currWriteCount << " Write Submit Count: " << write_submit_count << std::endl;
    #ifndef USE_ASYNC_IO
    std::cout << "Busy Time: " << busy_time / 1000 << "ms IO Time: " << io_time / 1000 << "ms" << " io rate:" << (double)io_time / busy_time << std::endl;
    std::cout << "Remove Page Time: " << remove_page_time << "ms" << std::endl;
    std::cout << "Busy Thread Num: " << busy_thread_num << std::endl;
    std::cout << "Inflight IO Num: " << m_submittedSubIoRequests.unsafe_size() << std::endl;
    #endif

    return true;
}

bool FileIO::BlockController::ShutDown() {
    std::lock_guard<std::mutex> lock(m_initMutex);
    #ifdef USE_ASYNC_IO
    SubIoRequest* currSubIo;
    m_numInitCalled--;
    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "FileIO::BlockController::ShutDown\n");
    if (m_numInitCalled == 0) {
        m_fileIoThreadExiting = true;
        pthread_join(m_fileIoTid, NULL);
        while (!m_blockAddresses.empty()) {
            AddressType currBlockAddress;
            m_blockAddresses.try_pop(currBlockAddress);
        }
    }
    m_idQueue.push(id);

    for (auto &sr : m_currIoContext.sub_io_requests) {
        sr.app_buff = nullptr;
        free(sr.io_buff);
        sr.io_buff = nullptr;
    }
    while(m_currIoContext.free_sub_io_requests.size()) {
        m_currIoContext.free_sub_io_requests.pop();
    }
    syscall(__NR_io_destroy, iocp);
    return true;
    #else
    SubIoRequest* currSubIo;
    while (m_currIoContext.in_flight) {
        if (m_currIoContext.completed_sub_io_requests.try_pop(currSubIo)) {
            currSubIo->app_buff = nullptr;
            m_currIoContext.free_sub_io_requests.push(currSubIo);
            m_currIoContext.in_flight--;
        }
    }
    while (m_currIoContext.free_sub_io_requests.unsafe_size() < m_ssdFileIoDepth) {
        SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "FileIO::BlockController::ShutDown: Remain %d write jobs, wait for them.\n", m_ssdFileIoDepth - m_currIoContext.free_sub_io_requests.unsafe_size());
        // if (m_writeCache->getCacheSize() < 10) {
        //     m_writeCache->getMapStat();
        // }
        usleep(1000);
    }
    m_numInitCalled--;
    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "FileIO::BlockController::ShutDown\n");
    if(m_numInitCalled == 0) {
        m_fileIoThreadExiting = true;
        delete m_threadPool;
        pthread_join(m_fileIoTid, NULL);
        while(!m_blockAddresses.empty()) {
            AddressType currBlockAddress;
            m_blockAddresses.try_pop(currBlockAddress);
        }
    }

    for (auto &sr : m_currIoContext.sub_io_requests) {
        sr.completed_sub_io_requests = nullptr;
        sr.app_buff = nullptr;
        free(sr.io_buff);
        sr.io_buff = nullptr;
    }
    m_currIoContext.free_sub_io_requests.clear();
    return true;
    #endif
}

} // namespace SPTAG::SPANN
