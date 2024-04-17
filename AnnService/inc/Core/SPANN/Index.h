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

#include <cstddef>
#include <cstring>
#include <functional>
#include <ratio>
#include <shared_mutex>

#include <zmq.hpp>

namespace SPTAG
{

    namespace Helper
    {
        class IniReader;
    }

    namespace SPANN
    {
        class NetworkJob : public Helper::ThreadPool::Job
        {
        public:
            zmq::message_t* request;
            zmq::message_t* reply;
            int* in_flight;
            double* latency;
            NetworkJob(zmq::message_t* request, zmq::message_t* reply, int* in_flight, double* latency = nullptr)
                : request(request), reply(reply), in_flight(in_flight), latency(latency) {}
            ~NetworkJob() {}
            inline void exec(IAbortOperation* p_abort) override {
                *in_flight = 0;
            }
        };

        class NetworkThreadPool : public Helper::ThreadPool
        {
        public:
            void initNetwork(int numberOfThreads, std::string& m_ipAddrFrontend) 
            {
                m_abort.SetAbort(false);
                for (int i = 0; i < numberOfThreads; i++)
                {
                    m_threads.emplace_back([this, m_ipAddrFrontend] {
                        zmq::context_t context(1);
                        zmq::socket_t clientSocket(context, ZMQ_REQ);
                        clientSocket.connect(m_ipAddrFrontend.c_str());
                        Job *j;
                        while (get(j))
                        {
                            try 
                            {
                                NetworkJob *nj = static_cast<NetworkJob*>(j);
                                currentJobs++;
                                auto t1 = std::chrono::high_resolution_clock::now();
                                clientSocket.send(*(nj->request));
                                clientSocket.recv(nj->reply);
                                auto t2 = std::chrono::high_resolution_clock::now();
                                if (nj->latency) *(nj->latency) = ((double)(std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()));
                                *(nj->in_flight) = 0;
                                currentJobs--;
                            }
                            catch (std::exception& e) {
                                LOG(Helper::LogLevel::LL_Error, "ThreadPool: exception in %s %s\n", typeid(*j).name(), e.what());
                            }
                            
                            delete j;
                        }
                        clientSocket.close();
                        context.shutdown();
                        context.close();
                    });
                }
            }
        };

        template<typename T>
        class Index : public VectorIndex
        {
        private:
            std::shared_ptr<VectorIndex> m_index;
            std::shared_ptr<std::uint64_t> m_vectorTranslateMap;
            std::unordered_map<std::string, std::string> m_headParameters;

            std::shared_ptr<IExtraSearcher> m_extraSearcher;

            std::vector<std::shared_ptr<IExtraSearcher>> m_extraSearchers;
            std::vector<std::shared_ptr<std::uint64_t>> m_vectorTranslateMaps;

            Options m_options;

            std::function<float(const T*, const T*, DimensionType)> m_fComputeDistance;
            int m_iBaseSquare;

            std::mutex m_dataAddLock;
            COMMON::VersionLabel m_versionMap;

            // If not Coord, than bind some port
            bool m_isCoordinator;

            std::shared_ptr<NetworkThreadPool> m_clientThreadPool;

            std::vector<std::shared_ptr<NetworkThreadPool>> m_clientThreadPoolDSPANN;

            std::vector<SPTAG::COMMON::Dataset<int>> mappingData;

            // Distributed KV Coordinator : maintain the map that where all vectors are stored
            // Given a key, Coordinator hash it, the hash value tells Coordinator where to read the value

            // For static searcher, we only need to directly send the read request to the specific node without converting the key

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
                if (!m_options.m_isLocal && (m_options.m_layers == 2||!m_options.m_isCoordinator)) {
                    auto headfiles = m_index->GetIndexFiles();
                    for (auto file : *headfiles) {
                        files->push_back(m_options.m_headIndexFolder + FolderSep + file);
                    }
                    if (m_options.m_excludehead) files->push_back(m_options.m_headIDFile);
                }
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

            void InitSPectrumNetWork() {
                m_clientThreadPoolDSPANN.resize(m_options.m_dspannIndexFileNum);
                int node = 4;
                for (int i = 0; i < m_options.m_dspannIndexFileNum; i++, node += 1) {
                    std::string addrPrefix = m_options.m_ipAddrFrontendDSPANN;
                    addrPrefix += std::to_string(node);
                    addrPrefix += ":8000";
                    LOG(Helper::LogLevel::LL_Info, "Connecting to %s\n", addrPrefix.c_str());
                    m_clientThreadPoolDSPANN[i] = std::make_shared<NetworkThreadPool>();
                    m_clientThreadPoolDSPANN[i]->initNetwork(m_options.m_searchThreadNum/m_options.m_dspannIndexFileNum, addrPrefix);
                }
            }

            ErrorCode SearchIndexSPectrumMulti(QueryResult &p_query, int dispatchedNode, SPANN::SearchStats* stats) {
                COMMON::QueryResultSet<T>* p_queryResults = (COMMON::QueryResultSet<T>*) & p_query;
                zmq::message_t request(m_options.m_dim * sizeof(T));
                zmq::message_t reply;
                int in_flight = 0;                
                memcpy(request.data(), (char*)p_query.GetTarget(), m_options.m_dim * sizeof(T));
                in_flight = 1;

                auto* curJob = new NetworkJob(&request, &reply, &in_flight);

                m_clientThreadPoolDSPANN[dispatchedNode]->add(curJob);

                while (in_flight != 0) {
                    std::this_thread::sleep_for(std::chrono::microseconds(10));
                }

                auto ptr = static_cast<char*>(reply.data());
                for (int j = 0; j < m_options.m_resultNum; j++) {
                    int VID;
                    float Dist;
                    memcpy((char *)&VID, ptr, sizeof(int));
                    memcpy((char *)&Dist, ptr + sizeof(int), sizeof(float));
                    ptr += sizeof(int);
                    ptr += sizeof(float);

                    if (VID == -1) break;
                    p_queryResults->AddPoint(VID, Dist);
                }
                            
                p_queryResults->SortResult();

                //record stats

                return ErrorCode::Success;
                
            }

            void InitDSPANNNetWork() {
                if (m_options.m_multinode) {
                    mappingData.resize(m_options.m_dspannIndexFileNum);
                    m_clientThreadPoolDSPANN.resize(m_options.m_dspannIndexFileNum * 2);
                    // m_clientThreadPoolDSPANN.resize(m_options.m_dspannIndexFileNum);

                    // each node gets a replica, 8000&8001 for the first 8002&80003 for the second
                    // for shard i, m_clientThreadPoolDSPANN[i*2] and m_clientThreadPoolDSPANN[i*2+1] are all for processing
                    int node = 4;
                    for (int i = 0; i < m_options.m_dspannIndexFileNum; i++, node += 1) {
                        std::string addrPrefix = m_options.m_ipAddrFrontendDSPANN;
                        addrPrefix += std::to_string(node);
                        addrPrefix += ":8000";
                        LOG(Helper::LogLevel::LL_Info, "Connecting to %s\n", addrPrefix.c_str());
                        m_clientThreadPoolDSPANN[i*2] = std::make_shared<NetworkThreadPool>();
                        m_clientThreadPoolDSPANN[i*2]->initNetwork(m_options.m_searchThreadNum/m_options.m_dspannIndexFileNum, addrPrefix);
                        // m_clientThreadPoolDSPANN[i] = std::make_shared<NetworkThreadPool>();
                        // m_clientThreadPoolDSPANN[i]->initNetwork(m_options.m_searchThreadNum, addrPrefix);

                        addrPrefix = m_options.m_ipAddrFrontendDSPANN;
                        addrPrefix += std::to_string(node);
                        addrPrefix += ":8002";
                        LOG(Helper::LogLevel::LL_Info, "Connecting to %s\n", addrPrefix.c_str());
                        m_clientThreadPoolDSPANN[i*2+1] = std::make_shared<NetworkThreadPool>();
                        m_clientThreadPoolDSPANN[i*2+1]->initNetwork(m_options.m_searchThreadNum/m_options.m_dspannIndexFileNum, addrPrefix);

                        std::string filename = m_options.m_dspannIndexLabelPrefix + std::to_string(i);
                        LOG(Helper::LogLevel::LL_Info, "Load From %s\n", filename.c_str());
                        auto ptr = f_createIO();
                        if (ptr == nullptr || !ptr->Initialize(filename.c_str(), std::ios::binary | std::ios::in)) {
                            LOG(Helper::LogLevel::LL_Info, "Initialize Mapping Error: %d\n", i);
                            exit(1);
                        }
                        mappingData[i].Load(ptr, m_options.m_datasetRowsInBlock, m_options.m_datasetCapacity);
                    }

                } else {
                    mappingData.resize(m_options.m_dspannIndexFileNum);

                    m_clientThreadPoolDSPANN.resize(m_options.m_dspannIndexFileNum);
                    int port = 8000;
                    for (int i = 0; i < m_options.m_dspannIndexFileNum; i++, port += 2) {
                        std::string addrPrefix = m_options.m_ipAddrFrontendDSPANN;
                        addrPrefix += std::to_string(port);
                        LOG(Helper::LogLevel::LL_Info, "Connecting to %s\n", addrPrefix.c_str());
                        m_clientThreadPoolDSPANN[i] = std::make_shared<NetworkThreadPool>();
                        m_clientThreadPoolDSPANN[i]->initNetwork(m_options.m_searchThreadNum, addrPrefix);

                        std::string filename = m_options.m_dspannIndexLabelPrefix + std::to_string(i);
                        LOG(Helper::LogLevel::LL_Info, "Load From %s\n", filename.c_str());
                        auto ptr = f_createIO();
                        if (ptr == nullptr || !ptr->Initialize(filename.c_str(), std::ios::binary | std::ios::in)) {
                            LOG(Helper::LogLevel::LL_Info, "Initialize Mapping Error: %d\n", i);
                            exit(1);
                        }
                        mappingData[i].Load(ptr, m_options.m_datasetRowsInBlock, m_options.m_datasetCapacity);
                    }
                }
            }

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

            ErrorCode SearchIndexShard(QueryResult &p_query, std::vector<int>& needToTraverse, int top, std::vector<double>& latency)
            {
                COMMON::QueryResultSet<T>* p_queryResults = (COMMON::QueryResultSet<T>*) & p_query;
                zmq::message_t* request[top];
                zmq::message_t* reply[top];
                std::vector<int> in_flight(top, 0);                    

                for (int i = 0; i < top; ++i) {
                    request[i] = new zmq::message_t(m_options.m_dim * sizeof(T));
                    reply[i] = new zmq::message_t();

                    memcpy(request[i]->data(), (char*)p_query.GetTarget(), m_options.m_dim * sizeof(T));

                    in_flight[i] = 1;

                    auto* curJob = new NetworkJob(request[i], reply[i], &in_flight[i]);
                    if (m_options.m_multinode) {
                        if (m_clientThreadPoolDSPANN[needToTraverse[i] * 2]->runningJobs() + m_clientThreadPoolDSPANN[needToTraverse[i] * 2]->jobsize() > m_clientThreadPoolDSPANN[needToTraverse[i] * 2 + 1]->runningJobs() + m_clientThreadPoolDSPANN[needToTraverse[i] * 2 + 1]->jobsize()) {
                            m_clientThreadPoolDSPANN[needToTraverse[i] * 2 + 1]->add(curJob);
                        } else m_clientThreadPoolDSPANN[needToTraverse[i] * 2]->add(curJob);
                    }
                    else 
                    m_clientThreadPoolDSPANN[needToTraverse[i]]->add(curJob);
                }

                bool notReady = true;

                std::vector<int> visit(top, 0);

                std::set<int> visited;

                while (notReady) {
                    for (int i = 0; i < top; ++i) {
                        if (in_flight[i] == 0 && visit[i] == 0) {
                            visit[i] = 1;
                            auto ptr = static_cast<char*>(reply[i]->data());
                            for (int j = 0; j < m_options.m_resultNum; j++) {
                                int VID;
                                float Dist;
                                memcpy((char *)&VID, ptr, sizeof(int));
                                memcpy((char *)&Dist, ptr + sizeof(int), sizeof(float));
                                ptr += sizeof(int);
                                ptr += sizeof(float);

                                if (VID == -1) break;
                                if (visited.find(*mappingData[needToTraverse[i]][VID]) != visited.end()) continue;
                                visited.insert(*mappingData[needToTraverse[i]][VID]);
                                p_queryResults->AddPoint(*mappingData[needToTraverse[i]][VID], Dist);
                            }
                            double processLatency;
                            memcpy((char *)&processLatency, ptr, sizeof(double));
                            latency[i] = processLatency;
                        }
                    }
                    notReady = false;
                    for (int i = 0; i < top; ++i) {
                        if (visit[i] != 1) notReady = true;
                    }
                }
                p_queryResults->SortResult();

                return ErrorCode::Success;
            }

            ErrorCode SearchIndexRemote(QueryResult &p_query, SearchStats* p_stats)
            {
                if (!m_isCoordinator && !m_options.m_isLocal) {
                    LOG(Helper::LogLevel::LL_Info, "not Coordinator, can't not search!\n");
                    return ErrorCode::EmptyIndex;
                }

                p_stats->m_layerCounts.resize(m_options.m_layers);
                auto t1 = std::chrono::high_resolution_clock::now();
                m_index->SearchIndex(p_query, p_stats->m_layerCounts[0]);
                auto t2 = std::chrono::high_resolution_clock::now();

                p_stats->m_headLatency = ((double)(std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count())) / 1000;

                COMMON::QueryResultSet<T>* p_queryResults;

                p_queryResults = (COMMON::QueryResultSet<T>*) & p_query;

                p_stats->m_compLatencys.resize(m_options.m_layers-1);
                p_stats->m_diskReadLatencys.resize(m_options.m_layers-1);
                p_stats->m_exLatencys.resize(m_options.m_layers-1);
                p_stats->m_diskReadPages.resize(m_options.m_layers-1);
                p_stats->m_exWaitLatencys.resize(m_options.m_layers-1);

                for (int layer = 0; layer < m_options.m_layers - 1; layer++) {
                    QueryResult p_Result(NULL, m_options.m_searchInternalResultNum, false);
                    COMMON::QueryResultSet<T>* p_tempResult = (COMMON::QueryResultSet<T>*) & p_Result;

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

                        if (m_vectorTranslateMaps[layer].get() != nullptr) {
                            res->VID = static_cast<SizeType>((m_vectorTranslateMaps[layer].get())[res->VID]);
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
                    if (m_options.m_distKV) {
                        //sending request to the distKV
                        // 
                        auto t3 = std::chrono::high_resolution_clock::now();

                        int postingIDSize = m_workspace->m_postingIDs.size();

                        zmq::message_t request((postingIDSize+1)*sizeof(int) + m_options.m_dim * sizeof(T) + sizeof(int));
                        zmq::message_t reply;
                        int in_flight = 0;

                        in_flight= 1;

                        auto ptr = static_cast<char*>(request.data());

                        memcpy(ptr, (char*)&postingIDSize, sizeof(int));

                        ptr += sizeof(int);

                        memcpy(ptr, (char*)m_workspace->m_postingIDs.data(), sizeof(int)*postingIDSize);

                        ptr += sizeof(int)*postingIDSize;

                        memcpy(ptr, p_queryResults->GetQuantizedTarget(), sizeof(T) * m_options.m_dim);

                        ptr += sizeof(T) * m_options.m_dim;

                        memcpy(ptr, (char*)&layer, sizeof(int));
                          
                        // wait for return result

                        auto* curJob = new NetworkJob(&request, &reply, &in_flight);
                        m_clientThreadPool->add(curJob);

                        while (in_flight != 0) {
                            std::this_thread::sleep_for(std::chrono::microseconds(5));
                        }

                        int resultLength = reply.size();
                        int resultSize = (resultLength - sizeof(double) - sizeof(double) - sizeof(double)) / 8;

                        ptr = static_cast<char*>(reply.data());

                        /** id & dist (ResultNum) **/

                        COMMON::QueryResultSet<T>& queryResults = *((COMMON::QueryResultSet<T>*) & p_queryResults);

                        for (int i = 0; i < resultSize; i++) {
                            p_queryResults->AddPoint(*(int *)(ptr) , *(float *)(ptr+4));
                            ptr += 8;
                        } 
                        double remoteLocalTime;
                        memcpy((char*)&remoteLocalTime, ptr, sizeof(double));

                        p_stats->m_diskReadLatencys[layer] = remoteLocalTime / 1000;

                        ptr+=8;

                        double remoteCompTime;
                        memcpy((char*)&remoteCompTime, ptr, sizeof(double));

                        ptr+=8;

                        p_stats->m_compLatencys[layer] = remoteCompTime / 1000;

                        double remoteWaitTime;
                        memcpy((char*)&remoteWaitTime, ptr, sizeof(double));

                        p_stats->m_exWaitLatencys[layer] = remoteWaitTime / 1000;

                        auto t4 = std::chrono::high_resolution_clock::now();

                        double remoteProcessTime = std::chrono::duration_cast<std::chrono::microseconds>(t4 - t3).count();

                        p_stats->m_exLatencys[layer] = remoteProcessTime / 1000;
                    } else if (!m_options.m_isLocal) {
                        /***Remote Transefer And Process**/
                        auto t3 = std::chrono::high_resolution_clock::now();
                        // int socket_fd = ClientConnect();
                        RemoteQueryProcess(*p_queryResults, m_workspace->m_postingIDs, m_readedHead, p_stats, layer);
                        // CloseConnect(socket_fd);
                        auto t4 = std::chrono::high_resolution_clock::now();

                        double remoteProcessTime = std::chrono::duration_cast<std::chrono::microseconds>(t4 - t3).count();

                        p_stats->m_exLatencys[layer] = remoteProcessTime / 1000;

                        if (m_options.m_remoteCalculation) {
                            p_stats->m_exLatencys[layer] = p_stats->m_exLatencys[layer] - p_stats->m_diskReadLatency - p_stats->m_compLatency;
                        } else {
                            p_stats->m_exLatencys[layer] = p_stats->m_exLatencys[layer] - p_stats->m_diskReadLatency;
                        }

                        p_stats->m_diskReadLatencys[layer] = p_stats->m_diskReadLatency;

                        p_stats->m_compLatencys[layer] = p_stats->m_compLatency;
                    } else {
                        auto t3 = std::chrono::high_resolution_clock::now();
                        m_extraSearchers[layer]->SearchIndex(m_workspace.get(), *p_queryResults, m_index, p_stats);
                        auto t4 = std::chrono::high_resolution_clock::now();
                        double localProcessTime = std::chrono::duration_cast<std::chrono::microseconds>(t4 - t3).count();
                        p_stats->m_exLatencys[layer] = localProcessTime / 1000;
                        p_stats->m_compLatencys[layer] = p_stats->m_compLatency;
                        p_stats->m_diskReadLatencys[layer] = p_stats->m_exLatencys[layer] - p_stats->m_compLatency;
                        p_stats->m_layerCounts[layer+1] = p_stats->m_totalListElementsCount;
                        p_stats->m_diskReadPages[layer] = p_stats->m_diskAccessCount;
                    }

                    p_tempResult->SortResult();
                    if (m_vectorTranslateMaps[layer].get() != nullptr) {
                        for (int i = 0; i < p_tempResult->GetResultNum(); ++i) {
                            auto res = p_tempResult->GetResult(i);
                            if (res->VID == -1) break;
                            p_queryResults->AddPoint(res->VID, res->Dist);
                        }
                    }
                    p_queryResults->SortResult();
                }
                return ErrorCode::Success;
            }

            inline void Serialize(char* ptr, QueryResult& p_queryResults, std::vector<int>& m_postingIDs, std::vector<int>& m_readedHead, int layer) {
                memcpy(ptr, p_queryResults.GetQuantizedTarget(), sizeof(T) * m_options.m_dim);
                int postingNum = m_postingIDs.size();
                memcpy(ptr + sizeof(T) * m_options.m_dim, &layer, sizeof(int));
                memcpy(ptr + sizeof(T) * m_options.m_dim + sizeof(int), &postingNum, sizeof(int));
                memcpy(ptr + sizeof(T) * m_options.m_dim + 2*sizeof(int), m_postingIDs.data(), sizeof(int) * postingNum);
                if (m_readedHead.size()!=0) {
                    int headNum = m_readedHead.size();
                    memcpy(ptr + sizeof(T) * m_options.m_dim + sizeof(int) * (postingNum + 2), &headNum, sizeof(int));
                    memcpy(ptr + sizeof(T) * m_options.m_dim + sizeof(int) * (postingNum + 3), m_readedHead.data(), sizeof(int) * headNum);
                }
            }

            ErrorCode RemoteQueryProcess(QueryResult& p_queryResults, std::vector<int>& m_postingIDs, std::vector<int>& m_readedHead, SearchStats* p_stats, int layer) 
            {
                /** Serialize target vector, to be searched postings, allready readed VID**/
                if (m_options.m_remoteCalculation) {
                    int msgLength = sizeof(T) * m_options.m_dim + 2*sizeof(int) + sizeof(int) * m_postingIDs.size() + sizeof(int) + sizeof(int) * m_readedHead.size();

                    std::string postingList(msgLength, '\0');
                    char* ptr = (char*)(postingList.c_str());
                    /** target vector(dim * valuetype) + searched postings(posting numbers + posting IDs) + searched head VIDs**/
                    Serialize(ptr , p_queryResults, m_postingIDs, m_readedHead, layer);

                    zmq::message_t request(msgLength);
                    zmq::message_t reply;

                    memcpy(request.data(), ptr, msgLength);

                    int in_flight = 1;

                    auto* curJob = new NetworkJob(&request, &reply, &in_flight);
                    m_clientThreadPool->add(curJob);

                    while (in_flight != 0) {
                        std::this_thread::sleep_for(std::chrono::microseconds(20));
                    }
                    // clientSocket->send(request);
                    // clientSocket->recv(&reply);

                    int resultLength = reply.size();
                    int resultSize = (resultLength - 20) / 8;

                    ptr = static_cast<char*>(reply.data());

                    /** id & dist (ResultNum) **/

                    COMMON::QueryResultSet<T>& queryResults = *((COMMON::QueryResultSet<T>*) & p_queryResults);

                    for (int i = 0; i < resultSize; i++) {
                        queryResults.AddPoint(*(int *)(ptr) , *(float *)(ptr+4));
                        ptr += 8;
                    }

                    p_stats->m_diskReadLatency = (*(double *)(ptr));

                    p_stats->m_compLatency = (*(double *)(ptr + 8));

                    p_stats->m_layerCounts[layer+1] = (*(int *)(ptr + 16));

                    p_stats->m_diskAccessCount = 0;

                    // p_stats->m_diskReadLatency = 0;
                    // p_stats->m_compLatency = 0;

                } else {
                    
                    int msgLength = sizeof(T) * m_options.m_dim + 2*sizeof(int) + sizeof(int) * m_postingIDs.size();
                    std::string postingList(1 * msgLength, '\0');
                    char* ptr = (char*)(postingList.c_str());
                    /** target vector(dim * valuetype) + searched postings(posting numbers + posting IDs) **/
                    std::vector<int> m_readedHead_temp;
                    Serialize(ptr , p_queryResults, m_postingIDs, m_readedHead_temp, layer);


                    zmq::message_t request(msgLength);
                    zmq::message_t reply;

                    memcpy(request.data(), ptr, msgLength);

                    int in_flight = 1;

                    auto* curJob = new NetworkJob(&request, &reply, &in_flight);
                    m_clientThreadPool->add(curJob);

                    while (in_flight != 0) {
                        std::this_thread::sleep_for(std::chrono::microseconds(20));
                    }

                    // clientSocket->send(request);
                    // clientSocket->recv(&reply);

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
                // int m_vectorInfoSize = sizeof(T) * m_options.m_dim + sizeof(int) + sizeof(uint8_t);
                int m_vectorInfoSize = sizeof(T) * m_options.m_dim + sizeof(int);

                // int m_metaDataSize = sizeof(int) + sizeof(uint8_t);
                int m_metaDataSize = sizeof(int);

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

            // int ClientConnect() {
            //     clientSocket.reset(new zmq::socket_t(context, ZMQ_REQ)); 

            //     clientSocket->connect(m_options.m_ipAddrFrontend.c_str());
            //     return 0;
            // }

            // int ClientClose() {
            //     clientSocket->close();
            //     context.shutdown();
            //     context.close();
            //     return 0;
            // }

            ErrorCode WorkerDSPANN() {
                LOG(Helper::LogLevel::LL_Info, "Start Worker DSPANN\n");
                zmq::context_t context(1);

                zmq::socket_t responder(context, ZMQ_REP);
                responder.connect(m_options.m_ipAddrBackend.c_str());

                while(1) {
                    int msg_size = m_options.m_dim * sizeof(T);
                        
                    char* vectorBuffer = new char[msg_size];

                    zmq::message_t reply;
                    responder.recv(&reply);

                    auto t1 = std::chrono::high_resolution_clock::now();

                    char* ptr = static_cast<char*>(reply.data());

                    memcpy(vectorBuffer, ptr, msg_size);
                    // copy vector

                    QueryResult result(NULL, m_options.m_searchInternalResultNum, false);

                    (*((COMMON::QueryResultSet<T>*)&result)).SetTarget(reinterpret_cast<T*>(vectorBuffer), m_pQuantizer);
                    result.Reset();

                    SearchStats stats;

                    m_options.m_isLocal = true;
                    SearchIndexRemote(result, &stats);

                    int K = m_options.m_resultNum;
                        
                    zmq::message_t request(K * (sizeof(int) + sizeof(float))+ sizeof(double));
                    COMMON::QueryResultSet<T>* queryResults = (COMMON::QueryResultSet<T>*) & result;

                    ptr = static_cast<char*>(request.data());
                    for (int i = 0; i < K; i++) {
                        auto res = queryResults->GetResult(i);
                        memcpy(ptr, (char *)&res->VID, sizeof(int));
                        memcpy(ptr+4, (char *)&res->Dist, sizeof(float));
                        ptr+=8;
                    }

                    auto t2 = std::chrono::high_resolution_clock::now();

                    double localProcessTime = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();
                    
                    localProcessTime /= 1000;

                    memcpy(ptr, (char *)&localProcessTime, sizeof(double));

                    responder.send(request);
                }
                return ErrorCode::Success;
            }

            void initDistKVNetWork() {
                m_clientThreadPoolDSPANN.resize(m_options.m_dspannIndexFileNum);
                int node = 0;
                for (int i = 0; i < m_options.m_dspannIndexFileNum; i++, node += 1) {
                    if (node != MyNodeId()) {
                        std::string addrPrefix = m_options.m_ipAddrFrontendDSPANN;
                        addrPrefix += std::to_string(node + 4);
                        addrPrefix += ":8000";
                        LOG(Helper::LogLevel::LL_Info, "Connecting to %s\n", addrPrefix.c_str());
                        m_clientThreadPoolDSPANN[i] = std::make_shared<NetworkThreadPool>();
                        m_clientThreadPoolDSPANN[i]->initNetwork(m_options.m_searchThreadNum, addrPrefix);
                    }
                }
                // Debug version
                // for (int i = 0; i < m_options.m_dspannIndexFileNum; i++, node += 1) {
                //     if (node != MyNodeId()) {
                //         std::string addrPrefix = m_options.m_ipAddrFrontendDSPANN;
                //         addrPrefix += std::to_string(node * 2);
                //         LOG(Helper::LogLevel::LL_Info, "Connecting to %s\n", addrPrefix.c_str());
                //         m_clientThreadPoolDSPANN[i] = std::make_shared<NetworkThreadPool>();
                //         m_clientThreadPoolDSPANN[i]->initNetwork(m_options.m_searchThreadNum, addrPrefix);
                //     }
                // }
            }

            int NodeHash(int key, int layer) {
                return key % m_options.m_dspannIndexFileNum;
            }

            int MyNodeId() {
                return m_options.m_myNodeId;
            }

            int GroupNum() {
                return m_options.m_dspannIndexFileNum;
            }

            ErrorCode WorkerDistKV() {
                LOG(Helper::LogLevel::LL_Info, "Start Worker DistKV\n");
                zmq::context_t context(1);

                zmq::socket_t responder(context, ZMQ_REP);
                responder.connect(m_options.m_ipAddrBackend.c_str());

                while(1) {
                    QueryResult p_Result(NULL, m_options.m_searchInternalResultNum, false);
                    COMMON::QueryResultSet<T>* queryResults = (COMMON::QueryResultSet<T>*) & p_Result;
                    zmq::message_t reply;
                    responder.recv(&reply);
                    auto t1 = std::chrono::high_resolution_clock::now();
                    // client request, a list of key and vector and layer
                    // worker request, a list of key and vector and and char "0"
                    int size;
                    char* ptr = static_cast<char*>(reply.data());
                    memcpy((char *)&size, ptr, sizeof(int));
                    ptr += sizeof(int);
                    std::vector<int> keys(size);
                    memcpy((char *)keys.data(), ptr, sizeof(int)*size);
                    ptr += sizeof(int)*size;
                        
                    char* vectorBuffer = new char[m_options.m_dim * sizeof(T)];

                    memcpy(vectorBuffer, ptr, m_options.m_dim * sizeof(T));

                    ptr += m_options.m_dim * sizeof(T);

                    int layer;

                    memcpy((char *)&layer, ptr, sizeof(int));

                    if (((size+2) * sizeof(int) + m_options.m_dim * sizeof(T)) == reply.size()) {
                        // client request, a list of 
                        std::vector<std::vector<int>> keys_eachNode(GroupNum());

                        for (auto key: keys) {
                            int node = NodeHash(key, layer);
                            keys_eachNode[node].push_back(key);
                        }

                        zmq::message_t* request[GroupNum()];
                        zmq::message_t* reply[GroupNum()];
                        std::vector<int> in_flight(GroupNum(), 0); 
                        std::vector<int> visit(GroupNum(), 0);
                        std::vector<double> realLatency(GroupNum());

                        if (m_workspace.get() == nullptr) {
                            m_workspace.reset(new ExtraWorkSpace());
                            m_workspace->Initialize(m_options.m_maxCheck, m_options.m_hashExp, m_options.m_searchInternalResultNum, min(m_options.m_postingPageLimit, m_options.m_searchPostingPageLimit + 1) << PageSizeEx, m_options.m_enableDataCompression);
                        }

                        for (int i = 0; i < GroupNum(); i++) {
                            if (i == MyNodeId()) {
                                continue;
                            } else {
                                if (keys_eachNode[i].size() != 0) {
                                    request[i] = new zmq::message_t(sizeof(int)*(keys_eachNode[i].size() +1) + m_options.m_dim * sizeof(T) + sizeof(int) + sizeof(char));
                                    reply[i] = new zmq::message_t();
                                    in_flight[i] = 1;

                                    ptr = static_cast<char*>(request[i]->data());

                                    int keys_size = keys_eachNode[i].size();

                                    memcpy(ptr, (char*)&keys_size, sizeof(int));
                                    
                                    ptr += sizeof(int);

                                    memcpy(ptr, (char*)keys_eachNode[i].data(), sizeof(int)*keys_eachNode[i].size());

                                    ptr += sizeof(int)*keys_eachNode[i].size();

                                    memcpy(ptr, (char*)vectorBuffer, m_options.m_dim * sizeof(T));

                                    ptr += m_options.m_dim * sizeof(T);

                                    memcpy(ptr, (char*)&layer, sizeof(int));

                                    ptr += sizeof(int);

                                    char code = 0;

                                    memcpy(ptr, (char*)&code, sizeof(char));

                                    auto* curJob = new NetworkJob(request[i], reply[i], &in_flight[i], &realLatency[i]);

                                    m_clientThreadPoolDSPANN[i]->add(curJob);
                                    
                                } else {
                                    visit[i] = 1;
                                }
                                //Send request
                            }
                        }
                        // Search local
                        auto t3 = std::chrono::high_resolution_clock::now();

                        m_workspace->m_deduper.clear();
                        m_workspace->m_postingIDs.clear();
                        // currently we exclude head from extraSearcher, so we do not need to add head information into m_deduper
                        if (keys_eachNode[MyNodeId()].size() != 0) {
                            for (int j = 0; j < keys_eachNode[MyNodeId()].size(); j++) {
                                m_workspace->m_postingIDs.push_back(keys_eachNode[MyNodeId()][j]);
                            }
                            p_Result.SetTarget(reinterpret_cast<T*>(vectorBuffer));
                            double compLatency = 0;
                            int scannedNum = 0;
                            m_extraSearchers[layer]->GetAndCompMultiPosting(m_workspace.get(), p_Result, compLatency, scannedNum, m_options);
                        }
                        visit[MyNodeId()] = 1;

                        auto t4 = std::chrono::high_resolution_clock::now();

                        double localProcessTime = ((double)(std::chrono::duration_cast<std::chrono::microseconds>(t4 - t3).count()));

                        // wait for return and merge result

                        auto t5 = std::chrono::high_resolution_clock::now();

                        bool notReady = true;

                        while (notReady) {
                            for (int i = 0; i < GroupNum(); ++i) {
                                if (in_flight[i] == 0 && visit[i] == 0) {
                                    visit[i] = 1;
                                    auto ptr = static_cast<char*>(reply[i]->data());
                                    for (int j = 0; j < m_options.m_searchInternalResultNum; j++) {
                                        int VID;
                                        float Dist;
                                        memcpy((char *)&VID, ptr, sizeof(int));
                                        memcpy((char *)&Dist, ptr + sizeof(int), sizeof(float));
                                        ptr += sizeof(int);
                                        ptr += sizeof(float);
                                        if (VID == -1) break;
                                        if (m_workspace->m_deduper.CheckAndSet(VID)) continue;
                                        queryResults->AddPoint(VID, Dist);
                                    }
                                    auto t7 = std::chrono::high_resolution_clock::now();
                                    double thisQueryTime = ((double)(std::chrono::duration_cast<std::chrono::microseconds>(t7 - t3).count()));
                                    double thisTime;
                                    memcpy((char *)&thisTime, ptr, sizeof(double));
                                    LOG(Helper::LogLevel::LL_Info, "Remote Process Time: %lf, Remote Wait Time: %lf, localProcessTime: %lf, realTransferTime: %lf\n", thisTime, thisQueryTime, localProcessTime, realLatency[i]);
                                }
                            }
                            notReady = false;
                            for (int i = 0; i < GroupNum(); ++i) {
                                if (visit[i] != 1) notReady = true;
                            }
                            if (notReady) std::this_thread::sleep_for(std::chrono::microseconds(5));
                        }
                        queryResults->SortResult();

                        auto t6 = std::chrono::high_resolution_clock::now();

                        double waitProcessTime = ((double)(std::chrono::duration_cast<std::chrono::microseconds>(t6 - t5).count()));

                        // return
                        int K = m_options.m_searchInternalResultNum;
                        zmq::message_t replyClient(K * (sizeof(int) + sizeof(float)) + sizeof(double) + sizeof(double) + sizeof(double));

                        ptr = static_cast<char*>(replyClient.data());
                        for (int i = 0; i < K; i++) {
                            auto res = queryResults->GetResult(i);
                            memcpy(ptr, (char *)&res->VID, sizeof(int));
                            memcpy(ptr+4, (char *)&res->Dist, sizeof(float));
                            ptr+=8;
                        }

                        auto t2 = std::chrono::high_resolution_clock::now();

                        double processTime = ((double)(std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()));

                        memcpy(ptr, (char *)&processTime, sizeof(double));

                        ptr += 8;

                        memcpy(ptr, &localProcessTime, sizeof(double));

                        ptr += 8;

                        memcpy(ptr, &waitProcessTime, sizeof(double));

                        responder.send(replyClient);

                    } else if (((size+2) * sizeof(int) + m_options.m_dim * sizeof(T) + 1) == reply.size()) {
                        // worker request
                        auto t1 = std::chrono::high_resolution_clock::now();
                        if (m_workspace.get() == nullptr) {
                            m_workspace.reset(new ExtraWorkSpace());
                            m_workspace->Initialize(m_options.m_maxCheck, m_options.m_hashExp, m_options.m_searchInternalResultNum, min(m_options.m_postingPageLimit, m_options.m_searchPostingPageLimit + 1) << PageSizeEx, m_options.m_enableDataCompression);
                        }
                        m_workspace->m_deduper.clear();
                        m_workspace->m_postingIDs.clear();
                        // currently we exclude head from extraSearcher, so we do not need to add head information into m_deduper
                        for (auto key: keys) {
                            m_workspace->m_postingIDs.push_back(key);
                        }
                        p_Result.SetTarget(reinterpret_cast<T*>(vectorBuffer));
                        double compLatency = 0;
                        int scannedNum = 0;
                        m_extraSearchers[layer]->GetAndCompMultiPosting(m_workspace.get(), p_Result, compLatency, scannedNum, m_options);

                        // Return result
                        queryResults->SortResult();

                        int K = m_options.m_searchInternalResultNum;
                        
                        zmq::message_t request(K * (sizeof(int) + sizeof(float)) + sizeof(double));

                        ptr = static_cast<char*>(request.data());
                        for (int i = 0; i < m_options.m_searchInternalResultNum; i++) {
                            auto res = queryResults->GetResult(i);
                            memcpy(ptr, (char *)&res->VID, sizeof(int));
                            memcpy(ptr+4, (char *)&res->Dist, sizeof(float));
                            ptr+=8;
                        }

                        auto t2 = std::chrono::high_resolution_clock::now();

                        double processTime = ((double)(std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()));

                        memcpy(ptr, (char *)&processTime, sizeof(double));


                        responder.send(request);

                    } else {
                        LOG(Helper::LogLevel::LL_Error, "Invalid msg: %d\n", reply.size());
                        exit(1);
                    }
                }
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

                        p_Result.SetTarget(reinterpret_cast<T*>(vectorBuffer));

                        int layer;
                        memcpy((char*)&layer, ptr + msg_size, sizeof(int));

                        int postingNum;
                        memcpy((char*)&postingNum, ptr + msg_size + sizeof(int), sizeof(int));

                        if (m_workspace.get() == nullptr) {
                            m_workspace.reset(new ExtraWorkSpace());
                            m_workspace->Initialize(m_options.m_maxCheck, m_options.m_hashExp, m_options.m_searchInternalResultNum, min(m_options.m_postingPageLimit, m_options.m_searchPostingPageLimit + 1) << PageSizeEx, m_options.m_enableDataCompression);
                        }
                        m_workspace->m_postingIDs.resize(postingNum);

                        memcpy((char*)m_workspace->m_postingIDs.data(), ptr + msg_size + 2*sizeof(int), sizeof(int) * postingNum);

                        // std::vector<std::string> postingLists;

                        int headNum;
                        memcpy((char*)&headNum, ptr + msg_size + 2*sizeof(int) + sizeof(int) * postingNum, sizeof(int));

                        std::vector<int> m_readedHead(headNum);
                        memcpy((char*)m_readedHead.data(), ptr + msg_size + 2*sizeof(int) + sizeof(int) * (postingNum + 1), sizeof(int) * headNum);

                        m_workspace->m_deduper.clear();

                        for (int i = 0; i < headNum; i++) {
                            m_workspace->m_deduper.CheckAndSet(m_readedHead[i]);
                        }

                        double compLatency = 0;
                        int scannedNum = 0;

                        auto t1 = std::chrono::high_resolution_clock::now();
                        // m_extraSearchers[layer]->GetMultiPosting(m_workspace.get(), postingIDs, &postingLists);
                        m_extraSearchers[layer]->GetAndCompMultiPosting(m_workspace.get(), p_Result, compLatency, scannedNum, m_options);
                        auto t2 = std::chrono::high_resolution_clock::now();

                        double processTime = ((double)(std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()));
                        processTime /= 1000;

                        double diskReadTime = processTime - compLatency;
                        double computeTime = compLatency;

                        queryResults->SortResult();

                        int resultSize = queryResults->GetResultNum();
                        
                        zmq::message_t request(resultSize * (sizeof(int) + sizeof(float)) + 2 * sizeof(double) + sizeof(int));

                        ptr = static_cast<char*>(request.data());

                        for (int i = 0; i < queryResults->GetResultNum(); ++i) {
                            auto res = queryResults->GetResult(i);
                            memcpy(ptr, (char *)&res->VID, sizeof(int));
                            memcpy(ptr+4, (char *)&res->Dist, sizeof(float));
                            ptr+=8;
                        }
                        memcpy(ptr, (char*)&diskReadTime, sizeof(double));

                        memcpy(ptr+8, ((char*)&computeTime), sizeof(double));

                        memcpy(ptr+16, ((char*)&scannedNum), sizeof(int));

                        responder.send(request);

                    } else {

                        int layer;
                        memcpy((char*)&layer, ptr + msg_size, sizeof(int));

                        int postingNum;
                        memcpy((char*)&postingNum, ptr + msg_size + sizeof(int), sizeof(int));

                        std::vector<int> postingIDs(postingNum);
                        memcpy((char*)postingIDs.data(), ptr + msg_size + 2*sizeof(int), sizeof(int) * postingNum);

                        std::vector<std::string> postingLists;

                        if (m_workspace.get() == nullptr) {
                            m_workspace.reset(new ExtraWorkSpace());
                            m_workspace->Initialize(m_options.m_maxCheck, m_options.m_hashExp, m_options.m_searchInternalResultNum, min(m_options.m_postingPageLimit, m_options.m_searchPostingPageLimit + 1) << PageSizeEx, m_options.m_enableDataCompression);
                        }

                        auto t1 = std::chrono::high_resolution_clock::now();
                        m_extraSearchers[layer]->GetMultiPosting(m_workspace.get(), postingIDs, &postingLists);
                        auto t2 = std::chrono::high_resolution_clock::now();
                        double diskReadTime = (std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()) / 1000;

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

                LOG(Helper::LogLevel::LL_Info, "Connecting Frontend: %s\n", m_options.m_ipAddrFrontend.c_str());
                LOG(Helper::LogLevel::LL_Info, "Connecting Backend: %s\n", m_options.m_ipAddrBackend.c_str());
                frontend.bind(m_options.m_ipAddrFrontend.c_str());
                backend.bind(m_options.m_ipAddrBackend.c_str());

                //  Initialize poll set
                zmq::pollitem_t items [] = {
                    { frontend, 0, ZMQ_POLLIN, 0 },
                    { backend, 0, ZMQ_POLLIN, 0 }
                };

                initDistKVNetWork();

                std::vector<std::thread> m_threads;
                for (int i = 0; i < m_options.m_searchThreadNum; i++)
                {
                    m_threads.emplace_back([this] {
                        if (m_options.m_dspann) 
                            if (m_options.m_distKV)
                                WorkerDistKV();
                            else 
                                WorkerDSPANN();
                        else Worker();
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
