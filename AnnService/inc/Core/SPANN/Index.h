// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_SPANN_INDEX_H_
#define _SPTAG_SPANN_INDEX_H_

#include "inc/Core/Common.h"
#include "inc/Core/VectorIndex.h"

#include "inc/Core/Common/CommonUtils.h"
#include "inc/Core/Common/DistanceUtils.h"
#include "inc/Core/Common/SIMDUtils.h"
#include "inc/Core/Common/QueryResultSet.h"
#include "inc/Core/Common/BKTree.h"
#include "inc/Core/Common/WorkSpacePool.h"

#include "inc/Core/Common/Labelset.h"
#include "inc/Helper/SimpleIniReader.h"
#include "inc/Helper/StringConvert.h"
#include "inc/Helper/ThreadPool.h"
#include "inc/Helper/ConcurrentSet.h"
#include "inc/Helper/VectorSetReader.h"
#include "inc/Core/Common/IQuantizer.h"

#include "IExtraSearcher.h"
#include "Options.h"

#include <functional>
#include <ratio>
#include <shared_mutex>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <zmq.hpp>
#include <future>

namespace SPTAG
{

    namespace Helper
    {
        class IniReader;
    }

    namespace SPANN
    {
        template<typename T>
        class Index : public VectorIndex
        {
        private:
            std::shared_ptr<VectorIndex> m_index;
            std::shared_ptr<std::uint64_t> m_vectorTranslateMap;
            std::unordered_map<std::string, std::string> m_headParameters;

            std::shared_ptr<IExtraSearcher> m_extraSearcher;

            Options m_options;

            std::function<float(const T*, const T*, DimensionType)> m_fComputeDistance;
            int m_iBaseSquare;

            std::mutex m_dataAddLock;
            COMMON::VersionLabel m_versionMap;

            // If not Coord, than bind some port
            bool m_isCoordinator;

            std::shared_ptr<Helper::ThreadPool> m_clientThreadPool;

        public:
            static thread_local std::shared_ptr<ExtraWorkSpace> m_workspace;
            static thread_local std::shared_ptr<zmq::socket_t> clientSocket;
            static thread_local zmq::context_t context; 

        public:
            Index()
            {
                m_fComputeDistance = std::function<float(const T*, const T*, DimensionType)>(COMMON::DistanceCalcSelector<T>(m_options.m_distCalcMethod));
                m_iBaseSquare = (m_options.m_distCalcMethod == DistCalcMethod::Cosine) ? COMMON::Utils::GetBase<T>() * COMMON::Utils::GetBase<T>() : 1;
            }

            ~Index() {}

            inline std::shared_ptr<VectorIndex> GetMemoryIndex() { return m_index; }
            inline std::shared_ptr<IExtraSearcher> GetDiskIndex() { return m_extraSearcher; }
            inline Options* GetOptions() { return &m_options; }

            inline SizeType GetNumSamples() const { return m_versionMap.Count(); }
            inline DimensionType GetFeatureDim() const { return m_pQuantizer ? m_pQuantizer->ReconstructDim() : m_index->GetFeatureDim(); }
            inline SizeType GetValueSize() const { return m_options.m_dim * sizeof(T); }

            inline int GetCurrMaxCheck() const { return m_options.m_maxCheck; }
            inline int GetNumThreads() const { return m_options.m_iSSDNumberOfThreads; }
            inline DistCalcMethod GetDistCalcMethod() const { return m_options.m_distCalcMethod; }
            inline IndexAlgoType GetIndexAlgoType() const { return IndexAlgoType::SPANN; }
            inline VectorValueType GetVectorValueType() const { return GetEnumValueType<T>(); }
            
            void SetQuantizer(std::shared_ptr<SPTAG::COMMON::IQuantizer> quantizer);

            inline float AccurateDistance(const void* pX, const void* pY) const { 
                if (m_options.m_distCalcMethod == DistCalcMethod::L2) return m_fComputeDistance((const T*)pX, (const T*)pY, m_options.m_dim);

                float xy = m_iBaseSquare - m_fComputeDistance((const T*)pX, (const T*)pY, m_options.m_dim);
                float xx = m_iBaseSquare - m_fComputeDistance((const T*)pX, (const T*)pX, m_options.m_dim);
                float yy = m_iBaseSquare - m_fComputeDistance((const T*)pY, (const T*)pY, m_options.m_dim);
                return 1.0f - xy / (sqrt(xx) * sqrt(yy));
            }
            inline float ComputeDistance(const void* pX, const void* pY) const { return m_fComputeDistance((const T*)pX, (const T*)pY, m_options.m_dim); }
            inline bool ContainSample(const SizeType idx) const { return idx < m_options.m_vectorSize; }

            std::shared_ptr<std::vector<std::uint64_t>> BufferSize() const
            {
                std::shared_ptr<std::vector<std::uint64_t>> buffersize(new std::vector<std::uint64_t>);
                auto headIndexBufferSize = m_index->BufferSize();
                buffersize->insert(buffersize->end(), headIndexBufferSize->begin(), headIndexBufferSize->end());
                buffersize->push_back(sizeof(long long) * m_index->GetNumSamples());
                return std::move(buffersize);
            }

            std::shared_ptr<std::vector<std::string>> GetIndexFiles() const
            {
                std::shared_ptr<std::vector<std::string>> files(new std::vector<std::string>);
                auto headfiles = m_index->GetIndexFiles();
                for (auto file : *headfiles) {
                    files->push_back(m_options.m_headIndexFolder + FolderSep + file);
                }
                if (m_options.m_excludehead) files->push_back(m_options.m_headIDFile);
                return std::move(files);
            }

            ErrorCode SaveConfig(std::shared_ptr<Helper::DiskIO> p_configout);
            ErrorCode SaveIndexData(const std::vector<std::shared_ptr<Helper::DiskIO>>& p_indexStreams);

            ErrorCode LoadConfig(Helper::IniReader& p_reader);
            ErrorCode LoadIndexData(const std::vector<std::shared_ptr<Helper::DiskIO>>& p_indexStreams);
            ErrorCode LoadIndexDataFromMemory(const std::vector<ByteArray>& p_indexBlobs);

            ErrorCode BuildIndex(const void* p_data, SizeType p_vectorNum, DimensionType p_dimension, bool p_normalized = false, bool p_shareOwnership = false);
            ErrorCode BuildIndex(bool p_normalized = false);
            ErrorCode SearchIndex(QueryResult &p_query, bool p_searchDeleted = false) const;
            ErrorCode SearchDiskIndex(QueryResult& p_query, SearchStats* p_stats = nullptr) const;
            ErrorCode DebugSearchDiskIndex(QueryResult& p_query, int p_subInternalResultNum, int p_internalResultNum,
                SearchStats* p_stats = nullptr, std::set<int>* truth = nullptr, std::map<int, std::set<int>>* found = nullptr) const;
            ErrorCode UpdateIndex();

            ErrorCode SetParameter(const char* p_param, const char* p_value, const char* p_section = nullptr);
            std::string GetParameter(const char* p_param, const char* p_section = nullptr) const;

            inline const void* GetSample(const SizeType idx) const { return nullptr; }
            inline SizeType GetNumDeleted() const { return m_versionMap.GetDeleteCount(); }
            inline bool NeedRefine() const { return false; }
            ErrorCode RefineSearchIndex(QueryResult &p_query, bool p_searchDeleted = false) const { return ErrorCode::Undefined; }
            ErrorCode SearchTree(QueryResult& p_query) const { return ErrorCode::Undefined; }
            ErrorCode AddIndex(const void* p_data, SizeType p_vectorNum, DimensionType p_dimension, std::shared_ptr<MetadataSet> p_metadataSet, bool p_withMetaIndex = false, bool p_normalized = false);
            ErrorCode DeleteIndex(const SizeType& p_id);

            ErrorCode DeleteIndex(const void* p_vectors, SizeType p_vectorNum);
            ErrorCode RefineIndex(const std::vector<std::shared_ptr<Helper::DiskIO>>& p_indexStreams, IAbortOperation* p_abort) { return ErrorCode::Undefined; }
            ErrorCode RefineIndex(std::shared_ptr<VectorIndex>& p_newIndex) { return ErrorCode::Undefined; }
            
        private:
            bool CheckHeadIndexType();
            void SelectHeadAdjustOptions(int p_vectorCount);
            int SelectHeadDynamicallyInternal(const std::shared_ptr<COMMON::BKTree> p_tree, int p_nodeID, const Options& p_opts, std::vector<int>& p_selected);
            void SelectHeadDynamically(const std::shared_ptr<COMMON::BKTree> p_tree, int p_vectorCount, std::vector<int>& p_selected);

            template <typename InternalDataType>
            bool SelectHeadInternal(std::shared_ptr<Helper::VectorSetReader>& p_reader);

            ErrorCode BuildIndexInternal(std::shared_ptr<Helper::VectorSetReader>& p_reader);

        public:
            bool AllFinished() { if (m_options.m_useKV || m_options.m_useSPDK) return m_extraSearcher->AllFinished(); return true; }

            void GetDBStat() { 
                if (m_options.m_useKV || m_options.m_useSPDK) m_extraSearcher->GetDBStats(); 
                LOG(Helper::LogLevel::LL_Info, "Current Vector Num: %d, Deleted: %d .\n", GetNumSamples(), GetNumDeleted());
            }

            void GetIndexStat(int finishedInsert, bool cost, bool reset) { if (m_options.m_useKV || m_options.m_useSPDK) m_extraSearcher->GetIndexStats(finishedInsert, cost, reset); }
            
            void ForceCompaction() { if (m_options.m_useKV) m_extraSearcher->ForceCompaction(); }

            void StopMerge() { m_options.m_inPlace = true; }

            void OpenMerge() { m_options.m_inPlace = false; }

            void ForceGC() { m_extraSearcher->ForceGC(m_index.get()); }

            bool Initialize() { return m_extraSearcher->Initialize(); }

            bool ExitBlockController() { return m_extraSearcher->ExitBlockController(); }

            ErrorCode AddIndexSPFresh(const void *p_data, SizeType p_vectorNum, DimensionType p_dimension, SizeType* VID) {
                if ((!m_options.m_useKV &&!m_options.m_useSPDK) || m_extraSearcher == nullptr) {
                    LOG(Helper::LogLevel::LL_Error, "Only Support KV Extra Update\n");
                    return ErrorCode::Fail;
                }

                if (p_data == nullptr || p_vectorNum == 0 || p_dimension == 0) return ErrorCode::EmptyData;
                if (p_dimension != GetFeatureDim()) return ErrorCode::DimensionSizeMismatch;

                SizeType begin, end;
                {
                    std::lock_guard<std::mutex> lock(m_dataAddLock);

                    begin = m_versionMap.GetVectorNum();
                    end = begin + p_vectorNum;

                    if (begin == 0) { return ErrorCode::EmptyIndex; }

                    if (m_versionMap.AddBatch(p_vectorNum) != ErrorCode::Success) {
                        LOG(Helper::LogLevel::LL_Info, "MemoryOverFlow: VID: %d, Map Size:%d\n", begin, m_versionMap.BufferSize());
                        exit(1);
                    }
                }
                for (int i = 0; i < p_vectorNum; i++) VID[i] = begin + i;

                std::shared_ptr<VectorSet> vectorSet;
                if (m_options.m_distCalcMethod == DistCalcMethod::Cosine) {
                    ByteArray arr = ByteArray::Alloc(sizeof(T) * p_vectorNum * p_dimension);
                    memcpy(arr.Data(), p_data, sizeof(T) * p_vectorNum * p_dimension);
                    vectorSet.reset(new BasicVectorSet(arr, GetEnumValueType<T>(), p_dimension, p_vectorNum));
                    int base = COMMON::Utils::GetBase<T>();
                    for (SizeType i = 0; i < p_vectorNum; i++) {
                        COMMON::Utils::Normalize((T*)(vectorSet->GetVector(i)), p_dimension, base);
                    }
                }
                else {
                    vectorSet.reset(new BasicVectorSet(ByteArray((std::uint8_t*)p_data, sizeof(T) * p_vectorNum * p_dimension, false),
                        GetEnumValueType<T>(), p_dimension, p_vectorNum));
                }

                return m_extraSearcher->AddIndex(vectorSet, m_index, begin);
            }

            ErrorCode SearchIndexRemote(QueryResult &p_query, SearchStats* p_stats)
            {
                if (!m_isCoordinator) {
                    LOG(Helper::LogLevel::LL_Info, "not Coordinator, can't not search!\n");
                    return ErrorCode::EmptyIndex;
                }

                COMMON::QueryResultSet<T>* p_queryResults;
                QueryResult p_Result(NULL, m_options.m_searchInternalResultNum, false);
                COMMON::QueryResultSet<T>* p_tempResult = (COMMON::QueryResultSet<T>*) & p_Result;

                p_queryResults = (COMMON::QueryResultSet<T>*) & p_query;

                auto t1 = std::chrono::high_resolution_clock::now();
                m_index->SearchIndex(*p_queryResults);
                auto t2 = std::chrono::high_resolution_clock::now();

                p_stats->m_headLatency = ((double)(std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count())) / 1000;

                if (m_workspace.get() == nullptr) {
                    m_workspace.reset(new ExtraWorkSpace());
                    m_workspace->Initialize(m_options.m_maxCheck, m_options.m_hashExp, m_options.m_searchInternalResultNum, min(m_options.m_postingPageLimit, m_options.m_searchPostingPageLimit + 1) << PageSizeEx, m_options.m_enableDataCompression);
                }
                m_workspace->m_deduper.clear();
                m_workspace->m_postingIDs.clear();

                std::vector<int> m_readedHead;

                for (int i = 0; i < p_queryResults->GetResultNum(); ++i)
                {
                    auto res = p_queryResults->GetResult(i);
                    if (res->VID == -1) break;
                    
                    auto postingID = res->VID;
                    m_workspace->m_postingIDs.emplace_back(postingID);

                    if (m_vectorTranslateMap.get() != nullptr) {
                        res->VID = static_cast<SizeType>((m_vectorTranslateMap.get())[res->VID]);
                        if(!m_workspace->m_deduper.CheckAndSet(res->VID)) {
                            p_tempResult->AddPoint(res->VID, res->Dist);
                            m_readedHead.emplace_back(res->VID);
                        } 
                        res->VID = -1;
                        res->Dist = MaxDist;
                    }
                    else {
                        res->VID = -1;
                        res->Dist = MaxDist;
                    }
                }

                // if (m_vectorTranslateMap.get() != nullptr) p_queryResults->Reverse();



                /***Remote Transefer And Process**/
                auto t3 = std::chrono::high_resolution_clock::now();
                // int socket_fd = ClientConnect();
                RemoteQueryProcess(*p_queryResults, m_workspace->m_postingIDs, m_readedHead,  p_stats);
                // CloseConnect(socket_fd);
                auto t4 = std::chrono::high_resolution_clock::now();

                double remoteProcessTime = std::chrono::duration_cast<std::chrono::microseconds>(t4 - t3).count();

                p_stats->m_exLatency = remoteProcessTime / 1000;

                // p_stats->m_diskReadLatency = 0;

                p_tempResult->SortResult();
                if (m_vectorTranslateMap.get() != nullptr) {
                    for (int i = 0; i < p_tempResult->GetResultNum(); ++i) {
                        auto res = p_tempResult->GetResult(i);
                        if (res->VID == -1) break;
                        p_queryResults->AddPoint(res->VID, res->Dist);
                    }
                }
                p_queryResults->SortResult();
                return ErrorCode::Success;
            }

            inline void Serialize(char* ptr, QueryResult& p_queryResults, std::vector<int>& m_postingIDs, std::vector<int>& m_readedHead) {
                memcpy(ptr, p_queryResults.GetQuantizedTarget(), sizeof(T) * m_options.m_dim);
                int postingNum = m_postingIDs.size();
                memcpy(ptr + sizeof(T) * m_options.m_dim, &postingNum, sizeof(int));
                memcpy(ptr + sizeof(T) * m_options.m_dim + sizeof(int), m_postingIDs.data(), sizeof(int) * postingNum);
                if (m_readedHead.size()!=0) {
                    int headNum = m_readedHead.size();
                    memcpy(ptr + sizeof(T) * m_options.m_dim + sizeof(int) * (postingNum + 1), &headNum, sizeof(int));
                    memcpy(ptr + sizeof(T) * m_options.m_dim + sizeof(int) * (postingNum + 2), m_readedHead.data(), sizeof(int) * headNum);
                } 
            }

            ErrorCode RemoteQueryProcess(QueryResult& p_queryResults, std::vector<int>& m_postingIDs, std::vector<int>& m_readedHead, SearchStats* p_stats) 
            {
                /** Serialize target vector, to be searched postings, allready readed VID**/
                if (m_options.m_remoteCalculation) {
                    int msgLength = sizeof(T) * m_options.m_dim + sizeof(int) + sizeof(int) * m_postingIDs.size() + sizeof(int) + sizeof(int) * m_readedHead.size();

                    std::string postingList(msgLength, '\0');
                    char* ptr = (char*)(postingList.c_str());
                    /** target vector(dim * valuetype) + searched postings(posting numbers + posting IDs) + searched head VIDs**/
                    Serialize(ptr , p_queryResults, m_postingIDs, m_readedHead);

                    zmq::message_t request(msgLength);


                    memcpy(request.data(), ptr, msgLength);

                    clientSocket->send(request);

                    zmq::message_t reply;
                    clientSocket->recv(&reply);

                    int resultLength = reply.size();
                    int resultSize = (resultLength - 16) / 8;

                    ptr = static_cast<char*>(reply.data());

                    /** id & dist (ResultNum) **/

                    COMMON::QueryResultSet<T>& queryResults = *((COMMON::QueryResultSet<T>*) & p_queryResults);

                    for (int i = 0; i < resultSize; i++) {
                        queryResults.AddPoint(*(int *)(ptr) , *(float *)(ptr+4));
                        ptr += 8;
                    }

                    p_stats->m_diskReadLatency = (*(double *)(ptr));

                    p_stats->m_compLatency = (*(double *)(ptr + 8));

                    p_stats->m_diskAccessCount = msgLength;

                    // p_stats->m_diskReadLatency = 0;
                    // p_stats->m_compLatency = 0;

                } else {
                    
                    int msgLength = sizeof(T) * m_options.m_dim + sizeof(int) + sizeof(int) * m_postingIDs.size();
                    std::string postingList(1 * msgLength, '\0');
                    char* ptr = (char*)(postingList.c_str());
                    /** target vector(dim * valuetype) + searched postings(posting numbers + posting IDs) **/
                    std::vector<int> m_readedHead_temp;
                    Serialize(ptr , p_queryResults, m_postingIDs, m_readedHead_temp);


                    zmq::message_t request(msgLength);
                    memcpy(request.data(), ptr, msgLength);

                    clientSocket->send(request);

                    zmq::message_t reply;
                    clientSocket->recv(&reply);

                    ptr = static_cast<char*>(reply.data());
                    char msg_int[4];
                    memcpy(msg_int, ptr, 4);
                    int totalMsg_size = (*(int *)msg_int);

                    p_stats->m_diskAccessCount = totalMsg_size / 1024;

                    postingList.resize(totalMsg_size);
                    char* ptr_postingList = (char*)(postingList.c_str());
                    memcpy(ptr_postingList, ptr+4, totalMsg_size);
                    char msg_double[8];
                    memcpy(msg_double, ptr+4+totalMsg_size, sizeof(double));

                    p_stats->m_diskReadLatency = (*(double *)msg_double);

                    // p_stats->m_diskReadLatency = 0;

                    auto t1 = std::chrono::high_resolution_clock::now();

                    /**Process Vectors**/
                    ProcessPostingDSPANN(p_queryResults, postingList, m_readedHead);

                    auto t2 = std::chrono::high_resolution_clock::now();

                    double localProcessTime = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();

                    p_stats->m_compLatency = localProcessTime / 1000;

                }
                return ErrorCode::Success;
            }

            void ProcessPostingDSPANN(QueryResult& p_queryResults, 
                std::string& postingList, std::vector<int>& m_readedHead)
            {
                int m_vectorInfoSize = sizeof(T) * m_options.m_dim + sizeof(int) + sizeof(uint8_t);

                int m_metaDataSize = sizeof(int) + sizeof(uint8_t);

                int vectorNum = (int)(postingList.size() / m_vectorInfoSize);

                COMMON::QueryResultSet<T>& queryResults = *((COMMON::QueryResultSet<T>*) & p_queryResults);


                for (int i = 0; i < vectorNum; i++) {
                    char* vectorInfo = postingList.data() + i * m_vectorInfoSize;
                    int vectorID = *(reinterpret_cast<int*>(vectorInfo));

                    if(m_workspace->m_deduper.CheckAndSet(vectorID)) {
                        continue;
                    }
                    auto distance2leaf = m_index->ComputeDistance(queryResults.GetQuantizedTarget(), vectorInfo + m_metaDataSize);
                    queryResults.AddPoint(vectorID, distance2leaf);
                }
            }

            int ClientConnect() {
                clientSocket.reset(new zmq::socket_t(context, ZMQ_REQ)); 

                clientSocket->connect(m_options.m_ipAddrFrontend.c_str());
                return 0;
            }

            int ClientClose() {
                clientSocket->close();
                context.shutdown();
                context.close();
                return 0;
            }

            ErrorCode Worker() {
                LOG(Helper::LogLevel::LL_Info, "Start Worker\n");
                zmq::context_t context(1);

                zmq::socket_t responder(context, ZMQ_REP);
                responder.connect(m_options.m_ipAddrBackend.c_str());

                while(1) {
                    int msg_size = m_options.m_dim * sizeof(T);
                        
                    char* vectorBuffer = new char[msg_size];

                    zmq::message_t reply;
                    responder.recv(&reply);

                    char* ptr = static_cast<char*>(reply.data());

                    memcpy(vectorBuffer, ptr, msg_size);

                    if (m_options.m_remoteCalculation) {
                        QueryResult p_Result(NULL, m_options.m_searchInternalResultNum, false);
                        COMMON::QueryResultSet<T>* queryResults = (COMMON::QueryResultSet<T>*) & p_Result;

                        int postingNum;
                        memcpy((char*)&postingNum, ptr + msg_size, sizeof(int));

                        std::vector<int> postingIDs(postingNum);
                        memcpy((char*)postingIDs.data(), ptr + msg_size + sizeof(int), sizeof(int) * postingNum);

                        std::vector<std::string> postingLists;

                        auto t1 = std::chrono::high_resolution_clock::now();
                        m_extraSearcher->GetMultiPosting(postingIDs, &postingLists);
                        auto t2 = std::chrono::high_resolution_clock::now();
                        double diskReadTime = ((double)(std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count())) / 1000;

                        int headNum;
                        memcpy((char*)&headNum, ptr + msg_size + sizeof(int) + sizeof(int) * postingNum, sizeof(int));

                        std::vector<int> m_readedHead(headNum);
                        memcpy((char*)m_readedHead.data(), ptr + msg_size + sizeof(int) + sizeof(int) * (postingNum + 1), sizeof(int) * headNum);

                        if (m_workspace.get() == nullptr) {
                            m_workspace.reset(new ExtraWorkSpace());
                            m_workspace->Initialize(m_options.m_maxCheck, m_options.m_hashExp, m_options.m_searchInternalResultNum, min(m_options.m_postingPageLimit, m_options.m_searchPostingPageLimit + 1) << PageSizeEx, m_options.m_enableDataCompression);
                        }
                        m_workspace->m_deduper.clear();

                        for (int i = 0; i < headNum; i++) {
                            m_workspace->m_deduper.CheckAndSet(m_readedHead[i]);
                        }

                        int m_vectorInfoSize = sizeof(T) * m_options.m_dim + sizeof(int) + sizeof(uint8_t);

                        int m_metaDataSize = sizeof(int) + sizeof(uint8_t);

                        auto t3 = std::chrono::high_resolution_clock::now();

                        for (int j = 0; j < postingNum; j++) {

                            int vectorNum = (int)(postingLists[j].size() / m_vectorInfoSize);

                            for (int i = 0; i < vectorNum; i++) {
                                char* vectorInfo = postingLists[j].data() + i * m_vectorInfoSize;
                                int vectorID = *(reinterpret_cast<int*>(vectorInfo));

                                if(m_workspace->m_deduper.CheckAndSet(vectorID)) {
                                    continue;
                                }

                                auto distance2leaf = COMMON::DistanceUtils::ComputeDistance((const T*)vectorBuffer, (const T*)(vectorInfo + m_metaDataSize), m_options.m_dim , m_options.m_distCalcMethod);
                                queryResults->AddPoint(vectorID, distance2leaf);
                            }
                        }

                        queryResults->SortResult();

                        auto t4 = std::chrono::high_resolution_clock::now();

                        double computeTime = ((double)(std::chrono::duration_cast<std::chrono::microseconds>(t4 - t3).count())) / 1000;

                        int resultSize = queryResults->GetResultNum();
                        
                        zmq::message_t request(resultSize * (sizeof(int) + sizeof(float)) + 2 * sizeof(double));

                        ptr = static_cast<char*>(request.data());

                        for (int i = 0; i < queryResults->GetResultNum(); ++i) {
                            auto res = queryResults->GetResult(i);
                            memcpy(ptr, (char *)&res->VID, sizeof(int));
                            memcpy(ptr+4, (char *)&res->Dist, sizeof(float));
                            ptr+=8;
                        }
                        memcpy(ptr, (char*)&diskReadTime, sizeof(double));

                        memcpy(ptr+8, ((char*)&computeTime), sizeof(double));

                        responder.send(request);

                    } else {

                        int postingNum;
                        memcpy((char*)&postingNum, ptr + msg_size, sizeof(int));

                        std::vector<int> postingIDs(postingNum);
                        memcpy((char*)postingIDs.data(), ptr + msg_size + sizeof(int), sizeof(int) * postingNum);

                        std::vector<std::string> postingLists;

                        auto t1 = std::chrono::high_resolution_clock::now();
                        m_extraSearcher->GetMultiPosting(postingIDs, &postingLists);
                        auto t2 = std::chrono::high_resolution_clock::now();
                        double diskReadTime = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count() / 1000;

                        int totalSize = 0;
                        for (int i = 0; i < postingLists.size(); i++) {
                            totalSize += postingLists[i].size();
                        }

                        zmq::message_t request(totalSize+sizeof(double)+sizeof(int));

                        ptr = static_cast<char*>(request.data());
                        memcpy(ptr, (char*)&totalSize, sizeof(int));


                        int first = 0;
                        for (int i = 0; i < postingLists.size(); i++) {
                            // write(accept_socket, postingLists[i].data(), postingLists[i].size());
                            memcpy(ptr+sizeof(int)+first, postingLists[i].data(), postingLists[i].size());
                            first += postingLists[i].size();
                        }
                        memcpy(ptr+sizeof(int)+first, ((char*)&diskReadTime), sizeof(double));

                        responder.send(request);
                    }
                }
                return ErrorCode::Success;
            }

            ErrorCode BrokerOn() {
                LOG(Helper::LogLevel::LL_Info, "Start Broker\n");

                zmq::context_t context(1);
                zmq::socket_t frontend (context, ZMQ_ROUTER);
                zmq::socket_t backend (context, ZMQ_DEALER);

                frontend.bind(m_options.m_ipAddrFrontend.c_str());
                backend.bind(m_options.m_ipAddrBackend.c_str());

                //  Initialize poll set
                zmq::pollitem_t items [] = {
                    { frontend, 0, ZMQ_POLLIN, 0 },
                    { backend, 0, ZMQ_POLLIN, 0 }
                };

                std::vector<std::thread> m_threads;
                for (int i = 0; i < m_options.m_searchThreadNum; i++)
                {
                    m_threads.emplace_back([this] {
                        Worker();
                    });
                }
                
                //  Switch messages between sockets
                while (1) {
                    zmq::message_t message;
                    int more;               //  Multipart detection

                    zmq::poll (&items [0], 2, -1);
                    
                    if (items [0].revents & ZMQ_POLLIN) {
                        while (1) {
                            //  Process all parts of the message
                            frontend.recv(&message);
                            // frontend.recv(message, zmq::recv_flags::none); // new syntax
                            size_t more_size = sizeof (more);
                            frontend.getsockopt(ZMQ_RCVMORE, &more, &more_size);
                            backend.send(message, more? ZMQ_SNDMORE: 0);
                            // more = frontend.get(zmq::sockopt::rcvmore); // new syntax
                            // backend.send(message, more? zmq::send_flags::sndmore : zmq::send_flags::none);
                            
                            if (!more)
                                break;      //  Last message part
                        }
                    }
                    if (items [1].revents & ZMQ_POLLIN) {
                        while (1) {
                            //  Process all parts of the message
                            backend.recv(&message);
                            size_t more_size = sizeof (more);
                            backend.getsockopt(ZMQ_RCVMORE, &more, &more_size);
                            frontend.send(message, more? ZMQ_SNDMORE: 0);
                            // more = backend.get(zmq::sockopt::rcvmore); // new syntax
                            //frontend.send(message, more? zmq::send_flags::sndmore : zmq::send_flags::none);

                            if (!more)
                                break;      //  Last message part
                        }
                    }
                }            
                return ErrorCode::Success;
            }
        };
    } // namespace SPANN
} // namespace SPTAG

#endif // _SPTAG_SPANN_INDEX_H_
