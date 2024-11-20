#include "inc/Core/SPANN/ExtraFileController.h"

namespace SPTAG::SPANN
{
thread_local struct FileIO::BlockController::IoContext FileIO::BlockController::m_currIoContext;
int FileIO::BlockController::m_ssdInflight = 0;
int FileIO::BlockController::m_ioCompleteCount = 0;
std::unique_ptr<char[]> FileIO::BlockController::m_memBuffer;

void* FileIO::BlockController::InitializeFileIo(void* args) {
    FileIO::BlockController* ctrl = (FileIO::BlockController *)args;
    struct stat st;
    if(stat(filePath, &st) != 0) {
        std::ofstream file(filePath, std::ios::binary);
        file.seekp(kSsdImplMaxNumBlocks * PageSizeEx - 1);
        file.write("", 1);
        if(file.fail()) {
            fprintf(stderr, "FileIO::BlockController::InitializeFileIo failed\n");
            return nullptr;
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
            }
            file.close();
        }
    }
    const char* fileIoDepth = getenv(kFileIoDepth);
    if (fileIoDepth) ctrl->m_ssdFileIoDepth = atoi(fileIoDepth);

    if(ctrl->m_fileIoThreadStartFailed == false) {
        ctrl->m_fileIoThreadReady = true;
        // TODO: 这里要考虑下是否要创建一个新线程进行这个循环
        FileIoLoop(ctrl);
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
        sr.dma_buff = aligned_alloc(PageSize, PageSize);
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



} // namespace SPTAG::SPANN
