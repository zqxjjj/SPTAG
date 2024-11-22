#include "inc/Core/SPANN/ExtraFileController.h"

namespace SPTAG::SPANN
{
thread_local struct FileIO::BlockController::IoContext FileIO::BlockController::m_currIoContext;
int FileIO::BlockController::m_ssdInflight = 0;
int FileIO::BlockController::m_ioCompleteCount = 0;
std::unique_ptr<char[]> FileIO::BlockController::m_memBuffer;
#ifndef USE_HELPER_THREADPOOL

FileIO::BlockController::ThreadPool::ThreadPool(size_t numThreads, int fd, BlockController* ctrl) {
    this->fd = fd;
    this->ctrl = ctrl;
    stop = false;
    for (size_t i = 0; i < numThreads; i++) {
        workers.emplace_back(workerThread, this);
    }
}

FileIO::BlockController::ThreadPool::~ThreadPool() {
    stop = true;
    condition.notify_all();
    for (auto& worker : workers) {
        pthread_join(worker, nullptr);
    }
}

void FileIO::BlockController::ThreadPool::notify_one() {
    condition.notify_one();
}

void* FileIO::BlockController::ThreadPool::workerThread(void* arg) {
    ThreadPool* pool = (ThreadPool*)arg;
    pool->threadLoop();
    return nullptr;
}

void FileIO::BlockController::ThreadPool::threadLoop() {
    while(true) {
        {
            std::unique_lock<std::mutex> lock(queueMutex);
            condition.wait(lock, [this] { return stop || !ctrl->m_submittedSubIoRequests.empty();});
        }
        SubIoRequest* currSubIo = nullptr;
        while(ctrl->m_submittedSubIoRequests.try_pop(currSubIo)) {
            ctrl->m_ssdInflight++;
            if(currSubIo->is_read) {
                ssize_t bytesRead = pread(fd, currSubIo->io_buff, PageSize, currSubIo->offset);
                if(bytesRead == -1) {
                    fprintf(stderr, "pread failed");
                    stop = true;
                }
            }
            else {
                ssize_t bytesWritten = pwrite(fd, currSubIo->io_buff, PageSize, currSubIo->offset);
                if(bytesWritten == -1) {
                    fprintf(stderr, "pwrite failed");
                    stop = true;
                }
            }
            ctrl->m_ioCompleteCount++;
            currSubIo->completed_sub_io_requests->push(currSubIo);
            ctrl->m_ssdInflight--;
        }
        if(stop) {
            break;
        }
    }
}

#endif

void* FileIO::BlockController::InitializeFileIo(void* args) {
    FileIO::BlockController* ctrl = (FileIO::BlockController *)args;
    struct stat st;
    if(stat(filePath, &st) != 0) {
        std::ofstream file(filePath, std::ios::binary);
        file.seekp(kSsdImplMaxNumBlocks * PageSizeEx - 1);
        file.write("", 1);
        if(file.fail()) {
            fprintf(stderr, "FileIO::BlockController::InitializeFileIo failed\n");
            // return nullptr;
            ctrl->m_fileIoThreadStartFailed = true;
            fd = -1;
        }
        else {
            fd = open(filePath, O_RDWR | O_DIRECT);
        }
        file.close();
    }
    else {
        if(st.st_size < kSsdImplMaxNumBlocks * PageSizeEx) {
            std::ofstream file(filePath, std::ios::binary | std::ios::app);
            file.seekp(kSsdImplMaxNumBlocks * PageSizeEx - 1);
            file.write("", 1);
            if(file.fail()) {
                fprintf(stderr, "FileIO::BlockController::InitializeFileIo failed\n");
                ctrl->m_fileIoThreadStartFailed = true;
                fd = -1;
            }
            else {
                fd = open(filePath, O_RDWR | O_DIRECT);
            }
            file.close();
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
        ctrl->m_threadPool = new ThreadPool(ctrl->m_ssdFileIoThreadNum, fd, ctrl);
        m_ssdInflight = 0;
    }
    pthread_exit(NULL);
}

bool FileIO::BlockController::Initialize(int batchSize) {
    std::lock_guard<std::mutex> lock(m_initMutex);
    m_numInitCalled++;

    if(m_numInitCalled == 1) {
        m_batchSize = batchSize;
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
    m_currIoContext.sub_io_requests.resize(m_ssdFileIoDepth);
    m_currIoContext.in_flight = 0;
    for(auto &sr : m_currIoContext.sub_io_requests) {
        sr.completed_sub_io_requests = &(m_currIoContext.completed_sub_io_requests);
        sr.app_buff = nullptr;
        sr.io_buff = aligned_alloc(m_ssdFileIoAlignment, PageSize);
        sr.ctrl = this;
        m_currIoContext.free_sub_io_requests.push_back(&sr);
    }
    return true;
}

bool FileIO::BlockController::GetBlocks(AddressType* p_data, int p_size) {
    AddressType currBlockAddress = 0;
    for(int i = 0; i < p_size; i++) {
        while(!m_blockAddresses.try_pop(currBlockAddress));
        p_data[i] = currBlockAddress;
    }
    return true;
}

bool FileIO::BlockController::ReleaseBlocks(AddressType* p_data, int p_size) {
    for(int i = 0; i < p_size; i++) {
        m_blockAddresses_reserve.push(p_data[i]);
    }
    return true;
}

bool FileIO::BlockController::ReadBlocks(AddressType* p_data, std::string* p_value, const std::chrono::microseconds &timeout) {
    p_value->resize(p_data[0]);
    AddressType currOffset = 0;
    AddressType dataIdx = 1;
    SubIoRequest* currSubIo;

    // Clear timeout I/Os
    while (m_currIoContext.in_flight) {
        if (m_currIoContext.completed_sub_io_requests.try_pop(currSubIo)) {
            currSubIo->app_buff = nullptr;
            m_currIoContext.free_sub_io_requests.push_back(currSubIo);
            m_currIoContext.in_flight--;
        }
    }

    auto t1 = std::chrono::high_resolution_clock::now();
    while(currOffset < p_data[0] || m_currIoContext.in_flight) {
        auto t2 = std::chrono::high_resolution_clock::now();
        if(std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1) > timeout) {
            return false;
        }
        // Try submit
        if(currOffset < p_data[0] && m_currIoContext.free_sub_io_requests.size()) {
            currSubIo = m_currIoContext.free_sub_io_requests.back();
            m_currIoContext.free_sub_io_requests.pop_back();
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
            m_currIoContext.free_sub_io_requests.push_back(currSubIo);
            m_currIoContext.in_flight--;
        }
    }
    return true;
}

bool FileIO::BlockController::ReadBlocks(const std::vector<AddressType*>& p_data, std::vector<std::string>* p_values, const std::chrono::microseconds &timeout) {
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
            m_currIoContext.free_sub_io_requests.push_back(currSubIo);
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
                break;
            }
            // Try submit
            if(currSubIoIdx < currSubIoEndId && m_currIoContext.free_sub_io_requests.size()) {
                currSubIo = m_currIoContext.free_sub_io_requests.back();
                m_currIoContext.free_sub_io_requests.pop_back();
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
                m_currIoContext.free_sub_io_requests.push_back(currSubIo);
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
}

bool FileIO::BlockController::WriteBlocks(AddressType* p_data, int p_size, const std::string& p_value) {
    AddressType currBlockIdx = 0;
    int inflight = 0;
    SubIoRequest* currSubIo;
    int totalSize = p_value.size();
    // Submit all I/Os
    while(currBlockIdx < p_size || inflight) {
        // Try submit
        if(currBlockIdx < p_size && m_currIoContext.free_sub_io_requests.size()) {
            currSubIo = m_currIoContext.free_sub_io_requests.back();
            m_currIoContext.free_sub_io_requests.pop_back();
            currSubIo->app_buff = (void*)p_value.data() + currBlockIdx * PageSize;
            currSubIo->real_size = (PageSize * (currBlockIdx + 1)) > totalSize ? (totalSize - currBlockIdx * PageSize) : PageSize;
            currSubIo->is_read = false;
            currSubIo->offset = p_data[currBlockIdx] * PageSize;
            memcpy(currSubIo->io_buff, currSubIo->app_buff, currSubIo->real_size);
            m_submittedSubIoRequests.push(currSubIo);
            m_threadPool->notify_one();
            currBlockIdx++;
            inflight++;
        }
        // Try complete
        if(inflight && m_currIoContext.completed_sub_io_requests.try_pop(currSubIo)) {
            currSubIo->app_buff = nullptr;
            m_currIoContext.free_sub_io_requests.push_back(currSubIo);
            inflight--;
        }
    }
    return true;
}

bool FileIO::BlockController::IOStatistics() {
    int currIOCount = m_ioCompleteCount;
    int diffIOCount = currIOCount - m_preIOCompleteCount;
    m_preIOCompleteCount = currIOCount;

    auto currTime = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(currTime - m_preTime);
    m_preTime = currTime;

    double currIOPS = (double)diffIOCount * 1000 / duration.count();
    double currBandWidth = (double)diffIOCount * PageSize / 1024 * 1000 / 1024 * 1000 / duration.count();

    std::cout << "IOPS: " << currIOPS << "k Bandwidth: " << currBandWidth << "MB/s" << std::endl;

    return true;
}

bool FileIO::BlockController::ShutDown() {
    std::lock_guard<std::mutex> lock(m_initMutex);
    m_numInitCalled--;

    if(m_numInitCalled == 0) {
        m_fileIoThreadExiting = true;
        delete m_threadPool;
        pthread_join(m_fileIoTid, NULL);
        while(!m_blockAddresses.empty()) {
            AddressType currBlockAddress;
            m_blockAddresses.try_pop(currBlockAddress);
        }
    }

    SubIoRequest* currSubIo;
    while (m_currIoContext.in_flight) {
        if (m_currIoContext.completed_sub_io_requests.try_pop(currSubIo)) {
            currSubIo->app_buff = nullptr;
            m_currIoContext.free_sub_io_requests.push_back(currSubIo);
            m_currIoContext.in_flight--;
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
}

} // namespace SPTAG::SPANN
