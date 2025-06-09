// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_HELPER_ASYNCFILEREADER_H_
#define _SPTAG_HELPER_ASYNCFILEREADER_H_

#include "inc/Helper/DiskIO.h"
#include "inc/Helper/ConcurrentSet.h"
#include "inc/Core/Common.h"

#include <cstdint>
#include <functional>
#include <string>
#include <vector>
#include <thread>
#include <stdint.h>

#define ASYNC_READ 1
#define BATCH_READ 1

namespace SPTAG
{
    namespace Helper
    {
        void SetThreadAffinity(int threadID, std::thread& thread, NumaStrategy socketStrategy = NumaStrategy::LOCAL, OrderStrategy idStrategy = OrderStrategy::ASC);
#ifdef _MSC_VER
        class HandleWrapper
        {
        public:
            HandleWrapper(HANDLE p_handle) : m_handle(p_handle) {}
            HandleWrapper(HandleWrapper&& p_right) noexcept: m_handle(std::move(p_right.m_handle)) {}
            HandleWrapper() : m_handle(INVALID_HANDLE_VALUE) {}
            ~HandleWrapper() {}

            void Reset(HANDLE p_handle) { m_handle.reset(p_handle); }
            HANDLE GetHandle() { return m_handle.get(); }
            bool IsValid() const { return m_handle.get() != INVALID_HANDLE_VALUE; };
            void Close() { m_handle.reset(INVALID_HANDLE_VALUE); }

            HandleWrapper(const HandleWrapper&) = delete;
            HandleWrapper& operator=(const HandleWrapper&) = delete;

            struct HandleDeleter
            {
                void operator()(HANDLE p_handle) const
                {
                    if (p_handle != INVALID_HANDLE_VALUE)
                    {
                        ::CloseHandle(p_handle);
                    }

                    p_handle = INVALID_HANDLE_VALUE;
                }
            };

        private:
            typedef std::unique_ptr<typename std::remove_pointer<HANDLE>::type, HandleDeleter> UniqueHandle;
            UniqueHandle m_handle;
        };

        class RequestQueue
        {
        public:
            RequestQueue() {
                m_handle.Reset(::CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, NULL, 0));
            }

            void reset(int capacity) {}

            void push(AsyncReadRequest* j) {
                ::PostQueuedCompletionStatus(m_handle.GetHandle(),
                    0,
                    NULL,
                    reinterpret_cast<LPOVERLAPPED>(j));
            }

            bool pop(AsyncReadRequest*& j) {
                DWORD cBytes;
                ULONG_PTR key;
                OVERLAPPED* ol;
                BOOL ret = ::GetQueuedCompletionStatus(m_handle.GetHandle(),
                    &cBytes,
                    &key,
                    &ol,
                    INFINITE);
                if (FALSE == ret || nullptr == ol) return false;
                j = reinterpret_cast<AsyncReadRequest*>(ol);
                return true;
            }

            void* handle() {
                return m_handle.GetHandle();
            }

        private:
            HandleWrapper m_handle;
        };

        class AsyncFileIO : public DiskIO
        {
        public:
            AsyncFileIO(DiskIOScenario scenario = DiskIOScenario::DIS_UserRead) {}

            virtual ~AsyncFileIO() { ShutDown(); }

            virtual bool Initialize(const char* filePath, int openMode,
                std::uint64_t maxIOSize = (1 << 20),
                std::uint32_t maxReadRetries = 2,
                std::uint32_t maxWriteRetries = 2,
                std::uint16_t threadPoolSize = 4,
                std::uint64_t maxFileSize = (300ULL << 30))
            {
                m_fileHandle.Reset(::CreateFile(filePath,
                    GENERIC_READ,
                    FILE_SHARE_READ,
                    NULL,
                    OPEN_EXISTING,
                    FILE_FLAG_NO_BUFFERING | FILE_FLAG_OVERLAPPED,
                    NULL));

                if (!m_fileHandle.IsValid()) return false;

                m_diskSectorSize = static_cast<uint32_t>(GetSectorSize(filePath));
                SPTAGLIB_LOG(LogLevel::LL_Info, "Success open file handle: %s DiskSectorSize: %u\n", filePath, m_diskSectorSize);

                int iocpThreads = threadPoolSize;
                m_fileIocp.Reset(::CreateIoCompletionPort(m_fileHandle.GetHandle(), NULL, NULL, iocpThreads));
                for (int i = 0; i < iocpThreads; ++i)
                {
                    m_fileIocpThreads.emplace_back(std::thread(std::bind(&AsyncFileIO::ListionIOCP, this, i)));
                }
                return m_fileIocp.IsValid();
            }

            virtual std::uint64_t ReadBinary(std::uint64_t readSize, char* buffer, std::uint64_t offset = UINT64_MAX)
            {
                ResourceType resource;

                DiskUtils::CallbackOverLapped& col = resource.m_col;
                memset(&col, 0, sizeof(OVERLAPPED));
                col.Offset = (offset & 0xffffffff);
                col.OffsetHigh = (offset >> 32);
                col.m_data = nullptr;
                col.hEvent = CreateEvent(NULL, TRUE, FALSE, NULL);
                if (!::ReadFile(m_fileHandle.GetHandle(),
                    buffer,
                    static_cast<DWORD>(readSize),
                    nullptr,
                    &col) && GetLastError() != ERROR_IO_PENDING) {
                    if (col.hEvent) CloseHandle(col.hEvent);
                    return 0;
                }

                DWORD cBytes = 0;
                ::GetOverlappedResult(m_fileHandle.GetHandle(), &col, &cBytes, TRUE);
                if (col.hEvent) CloseHandle(col.hEvent);
                return cBytes;
            }

            virtual std::uint64_t WriteBinary(std::uint64_t writeSize, const char* buffer, std::uint64_t offset = UINT64_MAX)
            {
                return 0;
            }

            virtual std::uint64_t ReadString(std::uint64_t& readSize, std::unique_ptr<char[]>& buffer, char delim = '\n', std::uint64_t offset = UINT64_MAX)
            {
                return 0;
            }

            virtual std::uint64_t WriteString(const char* buffer, std::uint64_t offset = UINT64_MAX)
            {
                return 0;
            }

            virtual bool ReadFileAsync(AsyncReadRequest& readRequest)
            {
                DiskUtils::CallbackOverLapped& col = readRequest.myres.m_col;
                col.Offset = (readRequest.m_offset & 0xffffffff);
                col.OffsetHigh = (readRequest.m_offset >> 32);

                if (!::ReadFile(m_fileHandle.GetHandle(),
                    readRequest.m_buffer,
                    static_cast<DWORD>(readRequest.m_readSize),
                    nullptr,
                    &col) && GetLastError() != ERROR_IO_PENDING)
                {
                    return false;
                }
                return true;
            }

            virtual void Wait(AsyncReadRequest& readRequest)
            {
                // currently not used anywhere, effective only when ASYNC_READ is defined and BATCH_READ is not defined
                throw std::runtime_error("Not implemented");
            }

            virtual std::uint32_t BatchReadFile(AsyncReadRequest* readRequests, std::uint32_t requestCount, const std::chrono::microseconds& timeout, int batchSize = -1)
            {
                std::uint32_t remaining = requestCount;
                HANDLE completeQueue = readRequests[0].m_extension;
                for (std::uint32_t i = 0; i < requestCount; i++) {
                    AsyncReadRequest* readRequest = &(readRequests[i]);
                    if (!ReadFileAsync(*readRequest)) remaining--;
                }

                std::uint32_t totalRead = remaining;
                while (remaining > 0) {
                    DWORD cBytes;
                    ULONG_PTR key;
                    OVERLAPPED* ol;
                    BOOL ret = ::GetQueuedCompletionStatus(completeQueue,
                        &cBytes,
                        &key,
                        &ol,
                        INFINITE);
                    if (FALSE == ret || nullptr == ol) break;
                    remaining--;
                    AsyncReadRequest* req = reinterpret_cast<AsyncReadRequest*>(ol);
                    req->m_callback(true);

                }
                return totalRead;
            }

            virtual std::uint32_t BatchWriteFile(AsyncReadRequest* readRequests, std::uint32_t requestCount, const std::chrono::microseconds& timeout, int batchSize = -1)
            {
                return 0;
            }

            virtual std::uint64_t TellP() { return 0; }

            virtual void ShutDown()
            {
                m_fileHandle.Close();
                m_fileIocp.Close();

                for (auto& th : m_fileIocpThreads)
                {
                    if (th.joinable())
                    {
                        th.join();
                    }
                }
                m_fileIocpThreads.clear();
            }

        private:
            typedef DiskUtils::PrioritizedDiskFileReaderResource ResourceType;

            static uint64_t GetSectorSize(const char* p_filePath)
            {
                DWORD dwSectorSize = 0;
                LPTSTR lpFilePart;
                // Get file sector size
                DWORD dwSize = GetFullPathName(p_filePath, 0, NULL, &lpFilePart);
                if (dwSize > 0)
                {
                    LPTSTR lpBuffer = new TCHAR[dwSize];
                    if (lpBuffer)
                    {
                        DWORD dwResult = GetFullPathName(p_filePath, dwSize, lpBuffer, &lpFilePart);
                        if (dwResult > 0 && dwResult <= dwSize)
                        {
                            bool nameValid = false;
                            if (lpBuffer[0] == _T('\\'))
                            {
                                if (lpBuffer[1] == _T('\\'))
                                {
                                    DWORD i;
                                    if (dwSize > 2)
                                    {
                                        for (i = 2; lpBuffer[i] != 0 && lpBuffer[i] != _T('\\'); i++);
                                        if (lpBuffer[i] == _T('\\'))
                                        {
                                            for (i++; lpBuffer[i] != 0 && lpBuffer[i] != _T('\\'); i++);
                                            if (lpBuffer[i] == _T('\\'))
                                            {
                                                lpBuffer[i + 1] = 0;
                                                nameValid = true;
                                            }
                                        }
                                    }
                                }
                            }
                            else
                            {
                                if (((lpBuffer[0] >= _T('a') && lpBuffer[0] <= _T('z')) || (lpBuffer[0] >= _T('A') && lpBuffer[0] <= _T('Z'))) &&
                                    lpBuffer[1] == _T(':'))
                                {
                                    if (lpBuffer[2] != 0)
                                    {
                                        lpBuffer[2] = _T('\\'); lpBuffer[3] = 0;
                                        nameValid = true;
                                    }
                                }
                            }
                            if (nameValid)
                            {
                                DWORD dwSPC, dwNOFC, dwTNOC;
                                GetDiskFreeSpace(lpBuffer, &dwSPC, &dwSectorSize, &dwNOFC, &dwTNOC);
                            }
                        }
                        delete[] lpBuffer;
                    }
                }

                return dwSectorSize;
            }

            void ErrorExit()
            {
                LPVOID lpMsgBuf;
                DWORD dw = GetLastError();

                FormatMessage(
                    FORMAT_MESSAGE_ALLOCATE_BUFFER |
                    FORMAT_MESSAGE_FROM_SYSTEM |
                    FORMAT_MESSAGE_IGNORE_INSERTS,
                    NULL,
                    dw,
                    0,
                    (LPTSTR)&lpMsgBuf,
                    0, NULL);

                // Display the error message and exit the process

                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failed with: %s\n", (char*)lpMsgBuf);

                LocalFree(lpMsgBuf);
                ExitProcess(dw); 
                ShutDown();
            }

            void ListionIOCP(int i)
            {
                SetThreadAffinity(i, m_fileIocpThreads[i], NumaStrategy::SCATTER, OrderStrategy::DESC); // avoid IO threads overlap with search threads

                DWORD cBytes;
                ULONG_PTR key;
                OVERLAPPED* ol;
                DiskUtils::CallbackOverLapped* col;

                while (true)
                {
                    BOOL ret = ::GetQueuedCompletionStatus(this->m_fileIocp.GetHandle(),
                        &cBytes,
                        &key,
                        &ol,
                        INFINITE);
                    if (FALSE == ret || nullptr == ol)
                    {
                        //We exit the thread
                        return;
                    }

                    col = (DiskUtils::CallbackOverLapped*)ol;
                    auto req = static_cast<Helper::AsyncReadRequest*>(col->m_data);

                    if (nullptr != req)
                    {
                        ::PostQueuedCompletionStatus(req->m_extension, 0, NULL, reinterpret_cast<LPOVERLAPPED>(req));
                    }
                }
            }

        private:
            HandleWrapper m_fileHandle;

            HandleWrapper m_fileIocp;

            std::vector<std::thread> m_fileIocpThreads;

            uint32_t m_diskSectorSize = 512;
        };
#else
        extern struct timespec AIOTimeout;

        class RequestQueue
        {
        public:

            RequestQueue() :m_front(0), m_end(0), m_capacity(0) {}

            ~RequestQueue() {}

            void reset(int capacity) {
                if (capacity > m_capacity) {
                    m_capacity = capacity + 1;
                    m_queue.reset(new AsyncReadRequest * [m_capacity]);
                }
            }

            void push(AsyncReadRequest* j)
            {
                m_queue[m_end++] = j;
                if (m_end == m_capacity) m_end = 0;
            }

            bool pop(AsyncReadRequest*& j)
            {
                while (m_front == m_end) usleep(AIOTimeout.tv_nsec / 1000);
                j = m_queue[m_front++];
                if (m_front == m_capacity) m_front = 0;
                return true;
            }

            void* handle() 
            {
                return nullptr;
            }

        protected:
            int m_front, m_end, m_capacity;
            std::unique_ptr<AsyncReadRequest* []> m_queue;
        };

        class AsyncFileIO : public DiskIO
        {
        public:
            AsyncFileIO(DiskIOScenario scenario = DiskIOScenario::DIS_UserRead) {}

            virtual ~AsyncFileIO() { ShutDown(); }

            bool CreateFile(const char* filePath, uint64_t maxFileSize) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "AsyncFileIO: Create a file\n");
                m_fileHandle = open(filePath, O_CREAT | O_WRONLY, 0666);
                if (m_fileHandle == -1) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "AsyncFileIO::InitializeFileIo failed\n");
                    return false;
                }

                if (fallocate(m_fileHandle, 0, 0, maxFileSize) == -1) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "AsyncFileIO::InitializeFileIo failed: fallocate failed\n");
                    m_fileHandle = -1;
                    return false;
                }
                close(m_fileHandle);

                m_fileHandle = open(filePath, O_RDWR | O_DIRECT);
                if (m_fileHandle == -1) {
                    auto err_str = strerror(errno);
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "AsyncFileIO::InitializeFileIo failed: open failed:%s\n", err_str);
                    return false;
                }
                //SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "AsyncFileIO::InitializeFileIo: file %s created, fd=%d\n", filePath, m_fileHandle);
                return true;
            }

            virtual bool Initialize(const char* filePath, int openMode,
                std::uint64_t maxNumBlocks = (1 << 20),
                std::uint32_t maxReadRetries = 2,
                std::uint32_t maxWriteRetries = 2,
                std::uint16_t threadPoolSize = 4,
                std::uint64_t maxFileSize = (300ULL << 30))
            {
                if (!fileexists(filePath)) {
                    if (openMode == (O_RDONLY | O_DIRECT)) {
                        SPTAGLIB_LOG(LogLevel::LL_Error, "Failed to open file handle: %s\n", filePath);
                        return false;
                    }
                    if (!CreateFile(filePath, maxFileSize)) return false;
                }
                else {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "AsyncFileIO::Open a file\n");
                    auto actualFileSize = filesize(filePath);
                    if (openMode == (O_RDONLY | O_DIRECT) || actualFileSize == maxFileSize) {
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "AsyncFileIO::InitializeFileIo: file has been created with enough space.\n");
                        m_fileHandle = open(filePath, O_RDWR | O_DIRECT);
                        if (m_fileHandle == -1) {
                            auto err_str = strerror(errno);
                            SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "AsyncFileIO::Open failed: %s\n", err_str);
                            return false;
                        }
                    } else {
                        if (!CreateFile(filePath, maxFileSize)) return false;
                   }
                }
                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "AsyncFileIO::InitializeFileIo: file %s opened, fd=%d threads=%d maxNumBlocks=%d\n", filePath, m_fileHandle, threadPoolSize, maxNumBlocks);
                m_iocps.resize(threadPoolSize);
                memset(m_iocps.data(), 0, sizeof(aio_context_t) * threadPoolSize);
                for (int i = 0; i < threadPoolSize; i++) {
                    auto ret = syscall(__NR_io_setup, (int)maxNumBlocks, &(m_iocps[i]));
                    if (ret < 0) {
                        SPTAGLIB_LOG(LogLevel::LL_Error, "Cannot setup aio: %s\n", strerror(errno));
                        return false;
                    }
                }

#ifndef BATCH_READ
                m_shutdown = false;
                for (int i = 0; i < threadPoolSize; ++i)
                {
                    m_fileIocpThreads.emplace_back(std::thread(std::bind(&AsyncFileIO::ListionIOCP, this, i)));
                }
#endif
                return true;
            }

            virtual std::uint64_t ReadBinary(std::uint64_t readSize, char* buffer, std::uint64_t offset = UINT64_MAX)
            {
                return pread(m_fileHandle, (void*)buffer, readSize, offset);
            }

            virtual std::uint64_t WriteBinary(std::uint64_t writeSize, const char* buffer, std::uint64_t offset = UINT64_MAX)
            {
                return 0;
            }

            virtual std::uint64_t ReadString(std::uint64_t& readSize, std::unique_ptr<char[]>& buffer, char delim = '\n', std::uint64_t offset = UINT64_MAX)
            {
                return 0;
            }

            virtual std::uint64_t WriteString(const char* buffer, std::uint64_t offset = UINT64_MAX)
            {
                return 0;
            }

            virtual std::uint32_t BatchReadFile(AsyncReadRequest* readRequests, std::uint32_t requestCount, const std::chrono::microseconds& timeout, int batchSize = -1)
            {
                if (requestCount <= 0) return 0;

                auto t1 = std::chrono::high_resolution_clock::now();
                if (batchSize < 0 || batchSize > requestCount) batchSize = requestCount;
                std::vector<struct iocb*> iocbs(batchSize);
                std::vector<struct io_event> events(batchSize);
                int iocp = readRequests[0].m_status % m_iocps.size();
		struct timespec timeout_ts {0, 0};
		while (syscall(__NR_io_getevents, m_iocps[iocp], batchSize, batchSize, events.data(), &timeout_ts) > 0);
 
		/*
                while (m_currIoContext.free_sub_io_requests.size() < m_ssdFileIoDepth) {
                    int wait = m_ssdFileIoDepth - m_currIoContext.free_sub_io_requests.size();
                    std::vector<struct io_event> events(wait);
                    struct timespec timeout_ts {0, 0};
                    auto d = syscall(__NR_io_getevents, iocp, wait, wait, events.data(), &timeout_ts);
                    for (int i = 0; i < d; i++) {
                        auto req = reinterpret_cast<SubIoRequest*>(events[i].data);
                        req->app_buff = nullptr;
                        m_currIoContext.free_sub_io_requests.push(req);
                    }
                }
                */
                for (int i = 0; i < requestCount; i++) {
                    AsyncReadRequest* readRequest = readRequests + i;
                    struct iocb* myiocb = &(readRequest->myiocb);
 		    memset(myiocb, 0, sizeof(struct iocb));
                    myiocb->aio_data = reinterpret_cast<uintptr_t>(&readRequest);
                    myiocb->aio_lio_opcode = IOCB_CMD_PREAD;
                    myiocb->aio_fildes = m_fileHandle;
                    myiocb->aio_buf = (std::uint64_t)(readRequest->m_buffer);
                    //myiocb->aio_nbytes = readRequest->m_readSize;
                    myiocb->aio_nbytes = PageSize;
                    myiocb->aio_offset = static_cast<std::int64_t>(readRequest->m_offset);
                }
                uint32_t batchTotalDone = 0;
                for (int currSubIoStartId = 0; currSubIoStartId < requestCount; currSubIoStartId += batchSize) {
                    int currSubIoEndId = (currSubIoStartId + batchSize) > requestCount ? requestCount : currSubIoStartId + batchSize;
                    int totalToSubmit = currSubIoEndId - currSubIoStartId;
                    int totalSubmitted = 0, totalDone = 0;
                    for (int i = 0; i < totalToSubmit; i++) {
                        iocbs[i] = &(readRequests[currSubIoStartId + i].myiocb);
                    }

                    //SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "AsyncFileReader::ReadBlocks: totalToSubmit:%d\n", totalToSubmit);
                    while (totalDone < totalToSubmit) {
                        // Submit all I/Os
                        if (totalSubmitted < totalToSubmit) {
                            int s = syscall(__NR_io_submit, m_iocps[iocp], totalToSubmit - totalSubmitted, iocbs.data() + totalSubmitted);
                            if (s > 0) {
                                totalSubmitted += s;
                            } else {
                                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "AsyncFileReader::ReadBlocks: io_submit failed\n");
			    }
 
                        }
                        int wait = totalSubmitted - totalDone;
                        auto d = syscall(__NR_io_getevents, m_iocps[iocp], wait, wait, events.data() + totalDone, &AIOTimeout);
			if (d > 0) {
                            totalDone += d;
			}
                    }
                    batchTotalDone += totalDone;
                    auto t2 = std::chrono::high_resolution_clock::now();
                    if (std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1) > timeout) {
                        if (batchTotalDone < requestCount) {
                            SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "AsyncFileReader::ReadBlocks (batch[%d:%d]) : timeout\n", currSubIoStartId, currSubIoEndId);
                        }
                        break;
                    }
                }
                //SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "AsyncFileReader::ReadBlocks: finish\n");
                return batchTotalDone;
            }

            virtual std::uint32_t BatchWriteFile(AsyncReadRequest* readRequests, std::uint32_t requestCount, const std::chrono::microseconds& timeout, int batchSize = -1)
            {
                if (requestCount <= 0) return 0;

                auto t1 = std::chrono::high_resolution_clock::now();
                if (batchSize < 0 || batchSize > requestCount) batchSize = requestCount;
		
                std::vector<struct iocb*> iocbs(batchSize);
                std::vector<struct io_event> events(batchSize);
                int iocp = readRequests[0].m_status % m_iocps.size();
		struct timespec timeout_ts {0, 0};
		while (syscall(__NR_io_getevents, m_iocps[iocp], batchSize, batchSize, events.data(), &timeout_ts) > 0);
		
 
                for (int i = 0; i < requestCount; i++) {
                    AsyncReadRequest* readRequest = readRequests + i;
                    struct iocb* myiocb = &(readRequest->myiocb);
		    memset(myiocb, 0, sizeof(struct iocb));
                    myiocb->aio_data = reinterpret_cast<uintptr_t>(&readRequest);
                    myiocb->aio_lio_opcode = IOCB_CMD_PWRITE;
                    myiocb->aio_fildes = m_fileHandle;
                    myiocb->aio_buf = (std::uint64_t)(readRequest->m_buffer);
                    //myiocb->aio_nbytes = readRequest->m_readSize;
                    myiocb->aio_nbytes = PageSize;
                    myiocb->aio_offset = static_cast<std::int64_t>(readRequest->m_offset);
                }

                uint32_t batchTotalDone = 0;
                for (int currSubIoStartId = 0; currSubIoStartId < requestCount; currSubIoStartId += batchSize) {
                    int currSubIoEndId = (currSubIoStartId + batchSize) > requestCount ? requestCount : currSubIoStartId + batchSize;
                    int totalToSubmit = currSubIoEndId - currSubIoStartId;
                    int totalSubmitted = 0, totalDone = 0;
                    for (int i = 0; i < totalToSubmit; i++) {
                        iocbs[i] = &(readRequests[currSubIoStartId + i].myiocb);
                    }

                    //SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "AsyncFileReader::WriteBlocks: iocp:%d totalToSubmit:%d\n", iocp, totalToSubmit);
                    while (totalDone < totalToSubmit) {
                        // Submit all I/Os
                        if (totalSubmitted < totalToSubmit) {
                            int s = syscall(__NR_io_submit, m_iocps[iocp], totalToSubmit - totalSubmitted, iocbs.data() + totalSubmitted);
                            if (s > 0) {
                                totalSubmitted += s;
                            } else {
                                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "AsyncFileReader::WriteBlocks: io_submit failed\n");
			    }
                        }
                        int wait = totalSubmitted - totalDone;
                        auto d = syscall(__NR_io_getevents, m_iocps[iocp], wait, wait, events.data() + totalDone, nullptr);
			if (d > 0) {
                            totalDone += d;
                        }
                    }
                    batchTotalDone += totalDone;
                    auto t2 = std::chrono::high_resolution_clock::now();
                    if (std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1) > timeout) {
                        if (batchTotalDone < requestCount) {
                            SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "AsyncFileReader::ReadBlocks (batch[%d:%d]) : timeout\n", currSubIoStartId, currSubIoEndId);
                        }
                        break;
                    }
                }
                //SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "AsyncFileReader::WriteBlocks: finish\n");
                return batchTotalDone;
            }

            virtual bool ReadFileAsync(AsyncReadRequest& readRequest)
            {
                struct iocb *myiocb = &(readRequest.myiocb);
                myiocb->aio_data = reinterpret_cast<uintptr_t>(&readRequest);
                myiocb->aio_lio_opcode = IOCB_CMD_PREAD;
                myiocb->aio_fildes = m_fileHandle;
                myiocb->aio_buf = (std::uint64_t)(readRequest.m_buffer);
                myiocb->aio_nbytes = readRequest.m_readSize;
                myiocb->aio_offset = static_cast<std::int64_t>(readRequest.m_offset);

                struct iocb* iocbs[1] = { myiocb };
                int curTry = 0, maxTry = 10;
                while (curTry < maxTry && syscall(__NR_io_submit, m_iocps[(readRequest.m_status & 0xffff) % m_iocps.size()], 1, iocbs) < 1) {
                    usleep(AIOTimeout.tv_nsec / 1000);
                    curTry++;
                }
                if (curTry == maxTry) return false;
                return true;
            }

            virtual std::uint64_t TellP() { return 0; }

            virtual void ShutDown()
            {
                for (int i = 0; i < m_iocps.size(); i++) syscall(__NR_io_destroy, m_iocps[i]);
                close(m_fileHandle);
#ifndef BATCH_READ
                m_shutdown = true;
                for (auto& th : m_fileIocpThreads)
                {
                    if (th.joinable())
                    {
                        th.join();
                    }
                }
#endif
            }

            aio_context_t& GetIOCP(int i) { return m_iocps[i]; }

            int GetFileHandler() { return m_fileHandle; }

        private:
#ifndef BATCH_READ
            void ListionIOCP(int i) {
                int b = 10;
                std::vector<struct io_event> events(b);
                while (!m_shutdown)
                {
                    int numEvents = syscall(__NR_io_getevents, m_iocps[i], b, b, events.data(), &AIOTimeout);

                    for (int r = 0; r < numEvents; r++) {
                        AsyncReadRequest* req = reinterpret_cast<AsyncReadRequest*>((events[r].data));
                        if (nullptr != req)
                        {
                            req->m_callback(true);
                        }
                    }
                }
            }

            bool m_shutdown;

            std::vector<std::thread> m_fileIocpThreads;
#endif
            int m_fileHandle;

            std::vector<aio_context_t> m_iocps;
        };
#endif
        void BatchReadFileAsync(std::vector<std::shared_ptr<Helper::DiskIO>>& handlers, AsyncReadRequest* readRequests, int num);
    }
}

#endif // _SPTAG_HELPER_ASYNCFILEREADER_H_
