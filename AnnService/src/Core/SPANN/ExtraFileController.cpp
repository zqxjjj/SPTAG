#include "inc/Core/SPANN/ExtraFileController.h"

namespace SPTAG::SPANN
{
extern std::function<std::shared_ptr<Helper::DiskIO>(void)> f_createAsyncIO;
thread_local int FileIO::BlockController::debug_fd = -1;

bool FileIO::BlockController::Initialize(SPANN::Options &p_opt, bool recovery) {
    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "FileIO::BlockController::Initialize(%s, %d)\n", p_opt.m_spdkMappingPath.c_str(), p_opt.m_spdkBatchSize);
    std::lock_guard<std::mutex> lock(m_initMutex);
    m_numInitCalled++;

    if(m_numInitCalled == 1) {
        m_batchSize = p_opt.m_spdkBatchSize;
        if (m_filePath == nullptr) m_filePath = new char[1024];
	    strcpy(m_filePath, (p_opt.m_spdkMappingPath + "_postings").c_str());
        m_startTime = std::chrono::high_resolution_clock::now();

        int numblocks = max(p_opt.m_postingPageLimit, p_opt.m_searchPostingPageLimit + 1) * p_opt.m_searchInternalResultNum;
        m_fileHandle = f_createAsyncIO();
        if (m_fileHandle == nullptr || !m_fileHandle->Initialize(m_filePath,
#ifndef _MSC_VER
            O_RDWR | O_DIRECT, numblocks, 2, 2, p_opt.m_searchThreadNum, p_opt.m_maxFileSize << 30
#else
            GENERIC_READ, numblocks, 2, 2, (std::uint16_t)p_opt.m_ioThreads, (std::uint64_t)p_opt.m_maxFileSize << 30
#endif
        )) {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "FileIO::BlockController::Initialize failed\n");
            return false;
        }
        std::string blockpoolPath = (recovery) ? p_opt.m_persistentBufferPath : m_filePath;
        SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "FileIO::BlockController:Loading block pool from file:%s\n", blockpoolPath.c_str());
        ErrorCode ret = LoadBlockPool(blockpoolPath, (std::uint64_t)p_opt.m_maxFileSize << (30 - PageSizeEx), !recovery);
        if (ErrorCode::Success != ret) {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "FileIO::BlockController:Loading block pool failed!\n");
            return false;
        }
    }
 
#ifdef USE_FILE_DEBUG
    auto debug_file_name = std::string("/nvme1n1/lml/") + std::to_string(m_numInitCalled) + "_debug.log";
    debug_fd = open(debug_file_name.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0666);
    if (debug_fd == -1) {
        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "FileIO::BlockController::Initialize failed: open debug file failed\n");
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

bool FileIO::BlockController::ReadBlocks(AddressType* p_data, std::string* p_value, const std::chrono::microseconds &timeout, std::vector<Helper::AsyncReadRequest>* reqs) {
#ifdef USE_FILE_DEBUG
    auto debug_string = std::to_string(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now() - m_startTime).count()) + " 3";
    auto result = pwrite(debug_fd, debug_string.c_str(), debug_string.size(), 0);
    if (result == -1) {
        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "FileIO::BlockController::ReadBlocks: pwrite failed\n");
    }
    fsync(debug_fd);
#endif
    auto blockNum = (p_data[0] + PageSize - 1) >> PageSizeEx;
    if (blockNum > reqs->size()) {
        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "FileIO::BlockController::ReadBlocks: %d > %u\n", (int)blockNum, reqs->size());
        return false;
    }
    
    p_value->resize(p_data[0]);
    AddressType currOffset = 0;
    AddressType dataIdx = 1;
    for (int i = 0; i < blockNum; i++) {
        Helper::AsyncReadRequest& curr = reqs->at(i);
        curr.m_buffer = (char*)p_value->data() + currOffset;
        curr.m_readSize = (p_data[0] - currOffset) < PageSize ? (p_data[0] - currOffset) : PageSize;
        curr.m_offset = p_data[dataIdx] * PageSize;
        currOffset += PageSize;
        dataIdx++;
    }
    read_submit_vec += blockNum;
    std::uint32_t totalReads = m_fileHandle->BatchReadFile(reqs->data(), blockNum, timeout, m_batchSize);
    read_complete_vec += totalReads;
    if (totalReads < blockNum) {
        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "FileIO::BlockController::ReadBlocks: %u < %u\n", totalReads, blockNum);
        m_batchReadTimeouts++;
    }
    return true;
}

bool FileIO::BlockController::ReadBlocks(const std::vector<AddressType*>& p_data, std::vector<std::string>* p_values, const std::chrono::microseconds &timeout, std::vector<Helper::AsyncReadRequest>* reqs) {
#ifdef USE_FILE_DEBUG
    auto debug_string = std::to_string(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now() - m_startTime).count()) + " 4";
    auto result = pwrite(debug_fd, debug_string.c_str(), debug_string.size(), 0);
    if (result == -1) {
        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "FileIO::BlockController::ReadBlocks: pwrite failed\n");
    }
    fsync(debug_fd);
#endif
    auto t1 = std::chrono::high_resolution_clock::now();
    m_batchReadTimes++;
    p_values->resize(p_data.size());
    std::uint32_t reqcount = 0;
    for(size_t i = 0; i < p_data.size(); i++) {
        AddressType* p_data_i = p_data[i];
        std::string* p_value = &((*p_values)[i]);

        if (p_data_i == nullptr) {
            continue;
        }

        p_value->resize(p_data_i[0]);
        AddressType currOffset = 0;
        AddressType dataIdx = 1;
        while(currOffset < p_data_i[0]) {
            Helper::AsyncReadRequest& curr = reqs->at(reqcount);
            curr.m_buffer = (char*)p_value->data() + currOffset;
            curr.m_readSize = (p_data_i[0] - currOffset) < PageSize ? (p_data_i[0] - currOffset) : PageSize;
            curr.m_offset = p_data_i[dataIdx] * PageSize;
            currOffset += PageSize;
            dataIdx++;
            reqcount++;
            if (reqcount >= reqs->size()) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "FileIO::BlockController::ReadBlocks:  %u >= %u\n", reqcount, reqs->size());
                return false;
            }
        }
    }
    read_submit_vec += reqcount;
    std::uint32_t totalReads = m_fileHandle->BatchReadFile(reqs->data(), reqcount, timeout, m_batchSize);
    read_complete_vec += totalReads;
    if (totalReads < reqcount) {
        SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "FileIO::BlockController::ReadBlocks: %u < %u\n", totalReads, reqcount);
        m_batchReadTimeouts++;
    }
    return true;
}

bool FileIO::BlockController::ReadBlocks(const std::vector<AddressType*>& p_data, std::vector<Helper::PageBuffer<std::uint8_t>>& p_values, const std::chrono::microseconds& timeout, std::vector<Helper::AsyncReadRequest>* reqs) {
#ifdef USE_FILE_DEBUG
    auto debug_string = std::to_string(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now() - m_startTime).count()) + " 4";
    auto result = pwrite(debug_fd, debug_string.c_str(), debug_string.size(), 0);
    if (result == -1) {
        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "FileIO::BlockController::ReadBlocks: pwrite failed\n");
    }
    fsync(debug_fd);
#endif
    auto t1 = std::chrono::high_resolution_clock::now();
    m_batchReadTimes++;
    std::uint32_t reqcount = 0;
    for (size_t i = 0; i < p_data.size(); i++) {
        AddressType* p_data_i = p_data[i];
        std::uint8_t* p_value = p_values[i].GetBuffer();

        if (p_data_i == nullptr) {
            continue;
        }
        std::size_t postingSize = (std::size_t)p_data_i[0];
        p_values[i].SetAvailableSize(postingSize);
        AddressType currOffset = 0;
        AddressType dataIdx = 1;
        while (currOffset < postingSize) {
            if (dataIdx > p_values[i].GetPageSize()) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "FileIO::BlockController::ReadBlocks:  block(%d) > buffer page size (%d)\n", dataIdx - 1, p_values[i].GetPageSize());
                return false;
            }
            Helper::AsyncReadRequest& curr = reqs->at(reqcount);
            curr.m_buffer = (char*)p_value + currOffset;
            curr.m_readSize = (postingSize - currOffset) < PageSize ? (postingSize - currOffset) : PageSize;
            curr.m_offset = p_data_i[dataIdx] * PageSize;
            currOffset += PageSize;
            dataIdx++;
            reqcount++;
            if (reqcount >= reqs->size()) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "FileIO::BlockController::ReadBlocks:  %u >= %u\n", reqcount, reqs->size());
                return false;
            }
        }
    }
    read_submit_vec += reqcount;
    std::uint32_t totalReads = m_fileHandle->BatchReadFile(reqs->data(), reqcount, timeout, m_batchSize);
    read_complete_vec += totalReads;
    if (totalReads < reqcount) {
        SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "FileIO::BlockController::ReadBlocks: %u < %u\n", totalReads, reqcount);
        m_batchReadTimeouts++;
    }
    return true;
}

bool FileIO::BlockController::WriteBlocks(AddressType* p_data, int p_size, const std::string& p_value, const std::chrono::microseconds& timeout, std::vector<Helper::AsyncReadRequest>* reqs) {
#ifdef USE_FILE_DEBUG
    auto debug_string = std::to_string(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now() - m_startTime).count()) + " 5";
    auto result = pwrite(debug_fd, debug_string.c_str(), debug_string.size(), 0);
    if (result == -1) {
        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "FileIO::BlockController::WriteBlocks: pwrite failed\n");
    }
    fsync(debug_fd);
#endif

    AddressType currOffset = 0;
    int totalSize = p_value.size();
    for (int i = 0; i < p_size; i++) {
        Helper::AsyncReadRequest& curr = reqs->at(i);
        curr.m_buffer = (char*)p_value.data() + currOffset;
        curr.m_readSize = (totalSize - currOffset) < PageSize ? (totalSize - currOffset) : PageSize;
        curr.m_offset = p_data[i] * PageSize;
        currOffset += PageSize;
    }

    write_submit_vec += p_size;
    std::uint32_t totalWrites = m_fileHandle->BatchWriteFile(reqs->data(), p_size, timeout, m_batchSize);
    write_complete_vec += totalWrites;
    if (totalWrites < p_size) {
        SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "FileIO::BlockController::WriteBlocks: %u < %d\n", totalWrites, p_size);
    }
    return true;
}

bool FileIO::BlockController::IOStatistics() {
    int64_t currReadCount = read_complete_vec;
    int64_t read_submit_count = read_submit_vec;
    int64_t currWriteCount = write_complete_vec;
    int64_t write_submit_count = write_submit_vec;
    int64_t read_blocks_time = read_blocks_time_vec;
    int64_t read_bytes_count = read_bytes_vec;
    int64_t write_bytes_count = write_bytes_vec;
    
    int currIOCount = currReadCount + currWriteCount;
    int diffIOCount = currIOCount - m_preIOCompleteCount;
    m_preIOCompleteCount = currIOCount;

    int64_t currBytesCount = read_bytes_count + write_bytes_count;
    int64_t diffBytesCount = currBytesCount - m_preIOBytes;
    m_preIOBytes = currBytesCount;

    auto currTime = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(currTime - m_preTime);
    m_preTime = currTime;

    double currIOPS = (double)diffIOCount * 1000 / duration.count();
    double currBandWidth = (double)diffIOCount * PageSize / 1024 * 1000 / 1024 * 1000 / duration.count();
    // double currBandWidth = (double)diffBytesCount / 1024 * 1000 / 1024 * 1000 / duration.count();

    std::cout << "Diff IO Count: " << diffIOCount << " Time: " << duration.count() << "us" << std::endl;
    std::cout << "IOPS: " << currIOPS << "k Bandwidth: " << currBandWidth << "MB/s" << std::endl;
    std::cout << "Read Count: " << currReadCount << " Write Count: " << currWriteCount << " Read Submit Count: " << read_submit_count << " Write Submit Count: " << write_submit_count << std::endl;
    std::cout << "Read Bytes Count: " << read_bytes_count << " Write Bytes Count: " << write_bytes_count << std::endl;
    std::cout << "Read Blocks Time: " << read_blocks_time << "ns" << std::endl;
    std::cout << "Batch Read Times: " << m_batchReadTimes.load() << " Batch Read Timeouts: " << m_batchReadTimeouts.load() << std::endl;
    return true;
}

bool FileIO::BlockController::ShutDown() {
    std::lock_guard<std::mutex> lock(m_initMutex);
    m_numInitCalled--;
    if (m_numInitCalled == 0) {
        Checkpoint(m_filePath);
        while (!m_blockAddresses.empty()) {
            AddressType currBlockAddress;
            m_blockAddresses.try_pop(currBlockAddress);
        }
        SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "FileIO::BlockController::Close file handler\n");
        m_fileHandle->ShutDown();
    }
    return true;
}

} // namespace SPTAG::SPANN
