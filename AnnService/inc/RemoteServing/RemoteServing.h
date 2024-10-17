#include "inc/Core/Common.h"
#include "inc/Core/Common/TruthSet.h"
#include "inc/Core/SPANN/Index.h"
#include "inc/Core/VectorIndex.h"
#include "inc/Helper/SimpleIniReader.h"
#include "inc/Helper/StringConvert.h"
#include "inc/Helper/VectorSetReader.h"
#include "inc/SSDServing/Utils.h"
#include <cstdint>
#include <cstring>
#include <future>

#include <iomanip>
#include <iostream>
#include <fstream>
#include <ratio>

using namespace SPTAG;

namespace SPTAG {
	namespace RemoteServing {
        void LoadTruthLARGEVID(std::shared_ptr<SPTAG::Helper::DiskIO>& ptr, std::vector<std::set<SizeType>>& truth, int K, int& originalK, int p_iTruthNumber) {
            if (ptr->TellP() == 0) {
                int row;
                if (ptr->ReadBinary(4, (char*)&row) != 4 || ptr->ReadBinary(4, (char*)&originalK) != 4) {
                    LOG(Helper::LogLevel::LL_Error, "Fail to read truth file!\n");
                    exit(1);
                }
            }
            truth.clear();
            truth.resize(p_iTruthNumber);
            std::vector<int64_t> vec(originalK);
            for (int i = 0; i < p_iTruthNumber; i++)
            {
                if (ptr->ReadBinary(sizeof(int64_t) * originalK, (char*)vec.data()) != sizeof(int64_t) * originalK) {
                    LOG(Helper::LogLevel::LL_Error, "Truth number(%d) and query number(%d) are not match!\n", i, p_iTruthNumber);
                    exit(1);
                }
                truth[i].insert(vec.begin(), vec.begin() + K);
            }
        }

        template<typename T, typename V>
        void PrintPercentiles(const std::vector<V>& p_values, std::function<T(const V&)> p_get, const char* p_format)
        {
            double sum = 0;
            std::vector<T> collects;
            collects.reserve(p_values.size());
            for (const auto& v : p_values)
            {
                T tmp = p_get(v);
                sum += tmp;
                collects.push_back(tmp);
            }

            std::sort(collects.begin(), collects.end());

            LOG(Helper::LogLevel::LL_Info, "Avg\t50tiles\t90tiles\t95tiles\t99tiles\t99.9tiles\tMax\n");

            std::string formatStr("%.3lf");
            for (int i = 1; i < 7; ++i)
            {
                formatStr += '\t';
                formatStr += p_format;
            }

            formatStr += '\n';

            LOG(Helper::LogLevel::LL_Info,
                formatStr.c_str(),
                sum / collects.size(),
                collects[static_cast<size_t>(collects.size() * 0.50)],
                collects[static_cast<size_t>(collects.size() * 0.90)],
                collects[static_cast<size_t>(collects.size() * 0.95)],
                collects[static_cast<size_t>(collects.size() * 0.99)],
                collects[static_cast<size_t>(collects.size() * 0.999)],
                collects[static_cast<size_t>(collects.size() - 1)]);
        }

        std::shared_ptr<VectorSet> LoadQuerySet(SPANN::Options& p_opts)
        {
            LOG(Helper::LogLevel::LL_Info, "Start loading QuerySet...\n");
            std::shared_ptr<Helper::ReaderOptions> queryOptions(new Helper::ReaderOptions(p_opts.m_valueType, p_opts.m_dim, p_opts.m_queryType, p_opts.m_queryDelimiter));
            auto queryReader = Helper::VectorSetReader::CreateInstance(queryOptions);
            if (ErrorCode::Success != queryReader->LoadFile(p_opts.m_queryPath))
            {
                LOG(Helper::LogLevel::LL_Error, "Failed to read query file.\n");
                exit(1);
            }
            return queryReader->GetVectorSet();
        }

        void InitSPectrumNetWork(SPANN::Options& p_opts, std::vector<std::shared_ptr<SPANN::NetworkThreadPool>>& m_clientThreadPool) {
            // Here also contains Production Search Network Setting
            m_clientThreadPool.resize(p_opts.m_dspannIndexFileNum);
            // int node = 4;
            // for (int i = 0; i < m_options.m_dspannIndexFileNum; i++, node += 1) {
            //     std::string addrPrefix = m_options.m_ipAddrFrontendDSPANN;
            //     addrPrefix += std::to_string(node);
            //     addrPrefix += ":8002";
            //     LOG(Helper::LogLevel::LL_Info, "Connecting to %s\n", addrPrefix.c_str());
            //     m_clientThreadPoolDSPANN[i] = std::make_shared<NetworkThreadPool>();
            //     m_clientThreadPoolDSPANN[i]->initNetwork(m_options.m_socketThreadNum, addrPrefix);
            // }

            // Debug version
            // int node = 0;
            // for (int i = 0; i < p_opts.m_dspannIndexFileNum; i++, node += 1) {
            //     std::string addrPrefix = p_opts.m_ipAddrFrontendDSPANN;
            //     addrPrefix += std::to_string(node*2);
            //     LOG(Helper::LogLevel::LL_Info, "Connecting to %s\n", addrPrefix.c_str());
            //     m_clientThreadPool[i] = std::make_shared<SPANN::NetworkThreadPool>();
            //     m_clientThreadPool[i]->initNetwork(p_opts.m_socketThreadNum, addrPrefix);
            // }
            std::ifstream file(p_opts.m_topLayerNetConfigPath); 
            std::string ipAddr;
            int count = 0;

            if (file.is_open()) {
                while (std::getline(file, ipAddr)) {
                    LOG(Helper::LogLevel::LL_Info, "Connecting to %s\n", ipAddr.c_str());
                    m_clientThreadPool[count] = std::make_shared<SPANN::NetworkThreadPool>();
                    m_clientThreadPool[count]->initNetwork(p_opts.m_socketThreadNum, ipAddr);
                    count++;
                }
                file.close();
            } else {
                LOG(Helper::LogLevel::LL_Info, "Can not open topLayer Network Config FIle: %s\n", p_opts.m_topLayerNetConfigPath.c_str());
            }
            if (count < p_opts.m_dspannIndexFileNum) {
                LOG(Helper::LogLevel::LL_Info, "Can not get enough ip address, exit\n");
                exit(0);
            }
        }

        template <typename ValueType>
        ErrorCode SearchSPectrumRemote(SPANN::Options& p_opts, QueryResult &p_query, std::shared_ptr<SPANN::NetworkThreadPool> dispatchedNode, SPANN::SearchStats* stats) {
            COMMON::QueryResultSet<ValueType>* p_queryResults = (COMMON::QueryResultSet<ValueType>*) & p_query;
            std::string request;
            request.resize(p_opts.m_dim * sizeof(ValueType));
            std::string reply;
            int in_flight = 0;                
            memcpy(request.data(), (char*)p_query.GetTarget(), p_opts.m_dim * sizeof(ValueType));
            in_flight = 1;

            auto* curJob = new SPANN::NetworkJob(&request, &reply, &in_flight);

            dispatchedNode->add(curJob);

            while (in_flight != 0) {
                std::this_thread::sleep_for(std::chrono::microseconds(10));
            }

            auto ptr = static_cast<char*>(reply.data());
            for (int j = 0; j < p_opts.m_resultNum; j++) {
                SizeType VID;
                float Dist;
                memcpy((char *)&VID, ptr, sizeof(SizeType));
                memcpy((char *)&Dist, ptr + sizeof(SizeType), sizeof(float));
                ptr += sizeof(SizeType);
                ptr += sizeof(float);

                // LOG(Helper::LogLevel::LL_Info, "VID : %lu, Dist : %f\n", VID, Dist);
                if (VID == -1) break;
                p_queryResults->AddPoint(VID, Dist);
            }
            // exit(0);
                        
            p_queryResults->SortResult();

            //record stats

            memcpy((char*)&stats->m_totalLatency, ptr, sizeof(double));
            ptr += sizeof(double);

            memcpy((char*)&stats->m_headLatency, ptr, sizeof(double));
            ptr += sizeof(double);

            for (int layer = 0; layer < p_opts.m_layers - 1; layer++) {
                memcpy((char*)&stats->m_diskReadLatencys[layer], ptr, sizeof(double));
                ptr += sizeof(double);
            }

            return ErrorCode::Success;
            
        }

        template <typename ValueType>
        void SearchIndexShard(SPANN::Options& p_opts, QueryResult &p_query, std::vector<int>& needToTraverse, std::vector<std::shared_ptr<SPANN::NetworkThreadPool>>& m_clientThreadPoolDSPANN, int top, std::vector<double>& latency)
        {
            COMMON::QueryResultSet<ValueType>* p_queryResults = (COMMON::QueryResultSet<ValueType>*) & p_query;
            std::string* request[top];
            std::string* reply[top];
            std::vector<int> in_flight(top, 0);                    

            // LOG(Helper::LogLevel::LL_Info, "Sending to worker\n");
            for (int i = 0; i < top; ++i) {
                request[i] = new std::string();
                request[i]->resize(p_opts.m_dim * sizeof(ValueType));
                reply[i] = new std::string();

                memcpy(request[i]->data(), (char*)p_query.GetTarget(), p_opts.m_dim * sizeof(ValueType));

                in_flight[i] = 1;

                auto* curJob = new SPANN::NetworkJob(request[i], reply[i], &in_flight[i]);
                m_clientThreadPoolDSPANN[needToTraverse[i]]->add(curJob);
            }

            // LOG(Helper::LogLevel::LL_Info, "Waiting for result\n");
            bool notReady = true;
            std::vector<int> visit(top, 0);

            std::set<SizeType> visited;

            while (notReady) {
                for (int i = 0; i < top; ++i) {
                    if (in_flight[i] == 0 && visit[i] == 0) {
                        visit[i] = 1;
                        auto ptr = static_cast<char*>(reply[i]->data());
                        for (int j = 0; j < p_opts.m_resultNum; j++) {
                            SizeType VID;
                            float Dist;
                            memcpy((char *)&VID, ptr, sizeof(SizeType));
                            memcpy((char *)&Dist, ptr + sizeof(SizeType), sizeof(float));
                            ptr += sizeof(SizeType);
                            ptr += sizeof(float);

                            if (VID == -1) break;
                            if (visited.find(VID) != visited.end()) continue;
                            visited.insert(VID);
                            p_queryResults->AddPoint(VID, Dist);
                            // LOG(Helper::LogLevel::LL_Info, "VID: %lu, Dist: %f\n", VID, Dist);
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
            // exit(0);
            // LOG(Helper::LogLevel::LL_Info, "Sorting Result\n");
            p_queryResults->SortResult();
            return;
        }

        template <typename ValueType>
        void SPectrumSearchClient(SPANN::Index<ValueType>* p_index)
        {
            SPANN::Options& p_opts = *(p_index->GetOptions());

            std::vector<std::shared_ptr<SPANN::NetworkThreadPool>> m_clientThreadPool;

            InitSPectrumNetWork(p_opts, m_clientThreadPool);

            int internalResultNum = p_opts.m_searchInternalResultNum;
            auto querySet = LoadQuerySet(p_opts);

            SizeType numQueries = querySet->Count();

            std::string truthFile = p_opts.m_truthPath;

            int K = p_opts.m_resultNum;
            int truthK = (p_opts.m_truthResultNum <= 0) ? K : p_opts.m_truthResultNum;

            std::vector<QueryResult> results(numQueries, QueryResult(NULL, max(K, internalResultNum), false));
            std::vector<double> latency(numQueries);
            std::vector<SPANN::SearchStats> traverseLatency(numQueries);
            for (int i = 0; i < numQueries; ++i)
            {
                (*((COMMON::QueryResultSet<ValueType>*)&results[i])).SetTarget(reinterpret_cast<ValueType*>(querySet->GetVector(i)), p_index->m_pQuantizer);
                results[i].Reset();
            }
            LOG(Helper::LogLevel::LL_Info, "Load Query Finished\n");

            LOG(Helper::LogLevel::LL_Info, "Verifying connection\n");
            for (int i = 0; i < p_opts.m_dspannIndexFileNum; i++) {
                if (!m_clientThreadPool[i]->checkRemoteAvaliable()) {
                    exit(0);
                }
            }

            std::atomic_size_t queriesSent(0);

            std::vector<std::thread> threads;

            LOG(Helper::LogLevel::LL_Info, "Searching: numThread: %d, numQueries: %d.\n", p_opts.m_searchThreadNum, numQueries);

            SSDServing::Utils::StopW sw;

            if (p_opts.m_dspann) {
                ValueType* centers = (ValueType*)ALIGN_ALLOC(sizeof(ValueType) * p_opts.m_dspannIndexFileNum * p_opts.m_dim);
                {
                    auto ptr = f_createIO();
                    if (ptr == nullptr || !ptr->Initialize(p_opts.m_dspannCenters.c_str(), std::ios::binary | std::ios::in)) {
                        LOG(Helper::LogLevel::LL_Error, "Failed to read center file %s.\n", p_opts.m_dspannCenters.c_str());
                    }

                    int r;
                    DimensionType c;
                    DimensionType col = p_opts.m_dim;
                    int row = p_opts.m_dspannIndexFileNum;
                    ptr->ReadBinary(sizeof(int), (char*)&r) != sizeof(SizeType);
                    ptr->ReadBinary(sizeof(DimensionType), (char*)&c) != sizeof(DimensionType);

                    if (r != row || c != col) {
                        LOG(Helper::LogLevel::LL_Error, "Row(%d,%d) or Col(%d,%d) cannot match.\n", r, row, c, col);
                    }

                    ptr->ReadBinary(sizeof(ValueType) * row * col, (char*)centers);
                }
                LOG(Helper::LogLevel::LL_Info, "Load Center Finished\n");
                // Calculating query to shard

                int top = p_opts.m_dspannTopK;

                std::vector<std::vector<int>> needToTraverse(numQueries, std::vector<int>(top));

                std::vector<std::vector<double>> traverseLatencyDist(numQueries, std::vector<double>(top));

                struct ShardWithDist
                {
                    int id;

                    float dist;
                };

                for (int index = 0; index < numQueries; index++) {
                    std::vector<ShardWithDist> shardDist(p_opts.m_dspannIndexFileNum);
                    for (int j = 0; j < p_opts.m_dspannIndexFileNum; j++) {
                        float dist = COMMON::DistanceUtils::ComputeDistance((const ValueType*)querySet->GetVector(index), (const ValueType*)centers + j* p_opts.m_dim, querySet->Dimension(), p_index->GetDistCalcMethod());
                        shardDist[j].id = j;
                        shardDist[j].dist = dist;
                    }

                    std::sort(shardDist.begin(), shardDist.end(), [&](ShardWithDist& a, const ShardWithDist& b){
                        return a.dist == b.dist ? a.id < b.id : a.dist < b.dist;
                    });

                    for (int j = 0; j < top; j++) {
                        needToTraverse[index][j] = shardDist[j].id;
                    }
                }
                // Dispatching query to shard

                std::atomic_size_t queriesSent(0);

                std::vector<std::thread> threads;

                for (int i = 0; i < p_opts.m_searchThreadNum; i++) { threads.emplace_back([&, i]()
                    {
                        // Helper::SetThreadAffinity( ((i+1) * 4), threads[i], 0, 0);
                        // p_index->ClientConnect();
                        size_t index = 0;
                        while (true)
                        {
                            index = queriesSent.fetch_add(1);
                            if (index < numQueries)
                            {
                                if ((index & ((1 << 14) - 1)) == 0)
                                {
                                    LOG(Helper::LogLevel::LL_Info, "Sent %.2lf%%...\n", index * 100.0 / numQueries);
                                }
                                auto t1 = std::chrono::high_resolution_clock::now();
                                SearchIndexShard<ValueType>(p_opts, results[index], needToTraverse[index], m_clientThreadPool, top, traverseLatencyDist[index]);
                                auto t2 = std::chrono::high_resolution_clock::now();
                                double totalTime = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();
                                latency[index] = totalTime / 1000;
                            }
                            else
                            {
                                // p_index->ClientClose();
                                return;
                            }
                        }
                    });
                }
                for (auto& thread : threads) { thread.join(); }

                // Print Stats
                std::vector<double> minLatency(numQueries);
                std::vector<double> maxLatency(numQueries);

                for (int i = 0; i < numQueries; i++) {
                    minLatency[i] = traverseLatencyDist[i][0];
                    maxLatency[i] = traverseLatencyDist[i][0];
                    for (int j = 1; j < top; j++) {
                        if (traverseLatencyDist[i][j] > maxLatency[i]) maxLatency[i] = traverseLatencyDist[i][j];
                        if (traverseLatencyDist[i][j] < minLatency[i]) minLatency[i] = traverseLatencyDist[i][j];
                    }
                }

                LOG(Helper::LogLevel::LL_Info, "\nMin Latency Distirbution:\n");
                PrintPercentiles<double, double>(minLatency,
                    [](const double& ss) -> double
                    {
                        return ss;
                    },
                    "%.3lf");

                LOG(Helper::LogLevel::LL_Info, "\nMax Latency Distirbution:\n");
                PrintPercentiles<double, double>(maxLatency,
                    [](const double& ss) -> double
                    {
                        return ss;
                    },
                    "%.3lf");

            } else {
                for (int i = 0; i < p_opts.m_searchThreadNum; i++) { threads.emplace_back([&, i]()
                    {
                        size_t index = 0;
                        while (true)
                        {
                            index = queriesSent.fetch_add(1);
                            if (index < numQueries)
                            {
                                if ((index & ((1 << 14) - 1)) == 0)
                                {
                                    LOG(Helper::LogLevel::LL_Info, "Sent %.2lf%%...\n", index * 100.0 / numQueries);
                                }
                                traverseLatency[index].m_diskReadLatencys.resize(p_opts.m_layers-1);
                                auto t1 = std::chrono::high_resolution_clock::now();
                                SearchSPectrumRemote<ValueType>(p_opts, results[index], m_clientThreadPool[index % p_opts.m_dspannIndexFileNum], &traverseLatency[index]);
                                auto t2 = std::chrono::high_resolution_clock::now();
                                double totalTime = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();
                                latency[index] = totalTime / 1000;
                            }
                            else
                            {
                                return;
                            }
                        }
                    });
                }
                for (auto& thread : threads) { thread.join(); }
            }

            double sendingCost = sw.getElapsedSec();

            LOG(Helper::LogLevel::LL_Info,
                "Finish sending in %.3lf seconds, actuallQPS is %.2lf, query count %u.\n",
                sendingCost,
                numQueries / sendingCost,
                static_cast<uint32_t>(numQueries));
            
            float recall = 0, MRR = 0;
            std::vector<std::set<SizeType>> truth;
            if (!truthFile.empty())
            {
                LOG(Helper::LogLevel::LL_Info, "Start loading TruthFile...\n");

                auto ptr = f_createIO();
                if (ptr == nullptr || !ptr->Initialize(truthFile.c_str(), std::ios::in | std::ios::binary)) {
                    LOG(Helper::LogLevel::LL_Error, "Failed open truth file: %s\n", truthFile.c_str());
                    exit(1);
                }

                int originalK = truthK;
                if (p_opts.m_largeVID) {
                    LOG(Helper::LogLevel::LL_Info, "Loading truthSet with 8 byte id\n");
                    LoadTruthLARGEVID(ptr, truth, truthK, originalK, numQueries);
                } else {
                    COMMON::TruthSet::LoadTruth(ptr, truth, numQueries, originalK, truthK, p_opts.m_truthType);
                }
                // char tmp[4];
                // if (ptr->ReadBinary(4, tmp) == 4) {
                //     LOG(Helper::LogLevel::LL_Error, "Truth number is larger than query number(%d)!\n", numQueries);
                // }

                recall = COMMON::TruthSet::CalculateRecall<ValueType>(p_index, results, truth, K, truthK, querySet, nullptr, numQueries, nullptr, false, &MRR);
                LOG(Helper::LogLevel::LL_Info, "Recall%d@%d: %f MRR@%d: %f\n", truthK, K, recall, K, MRR);
            }

            LOG(Helper::LogLevel::LL_Info, "\nTotal Latency Distirbution:\n");
            PrintPercentiles<double, double>(latency,
                [](const double& ss) -> double
                {
                    return ss;
                },
                "%.3lf");

            if (!p_opts.m_dspann) {
                LOG(Helper::LogLevel::LL_Info, "\nTopLayerNode Total Latency Distirbution:\n");
                PrintPercentiles<double, SPANN::SearchStats>(traverseLatency,
                    [](const SPANN::SearchStats& ss) -> double
                    {
                        return ss.m_totalLatency;
                    },
                    "%.3lf");

                LOG(Helper::LogLevel::LL_Info, "\nTopLayerNode Head Latency Distirbution:\n");
                PrintPercentiles<double, SPANN::SearchStats>(traverseLatency,
                    [](const SPANN::SearchStats& ss) -> double
                    {
                        return ss.m_headLatency;
                    },
                    "%.3lf");

                for (int layer = 0; layer < p_opts.m_layers -1 ; layer++) {
                    for (int qID = 0; qID < numQueries; qID++) {
                        traverseLatency[qID].m_diskReadLatency = traverseLatency[qID].m_diskReadLatencys[layer];
                    }
                    LOG(Helper::LogLevel::LL_Info, "\nDisk Read Layer %d Latency Distirbution:\n", layer+1);
                    PrintPercentiles<double, SPANN::SearchStats>(traverseLatency,
                        [](const SPANN::SearchStats& ss) -> double
                        {
                            return ss.m_diskReadLatency;
                        },
                        "%.3lf");
                }
            }
        }

        template <typename ValueType>
        ErrorCode BrokerOn(SPANN::Index<ValueType>* p_index) {
            LOG(Helper::LogLevel::LL_Info, "Start Broker\n");

            SPANN::Options& p_opts = *(p_index->GetOptions());

            zmq::context_t context(1);
            zmq::socket_t frontend (context, ZMQ_ROUTER);
            zmq::socket_t backend (context, ZMQ_DEALER);

            LOG(Helper::LogLevel::LL_Info, "Connecting Frontend: %s\n", p_opts.m_ipAddrFrontend.c_str());
            LOG(Helper::LogLevel::LL_Info, "Connecting Backend: %s\n", p_opts.m_ipAddrBackend.c_str());
            frontend.bind(p_opts.m_ipAddrFrontend.c_str());
            backend.bind(p_opts.m_ipAddrBackend.c_str());

            //  Initialize poll set
            zmq::pollitem_t items [] = {
                { frontend, 0, ZMQ_POLLIN, 0 },
                { backend, 0, ZMQ_POLLIN, 0 }
            };

            // TopLayer connect to all nodes
            if (p_opts.m_isCoordinator) p_index->initDistKVNetWork();

            if (p_opts.m_distKV) p_index->testDistKV();

            std::vector<std::thread> m_threads;
            for (int i = 0; i < p_opts.m_searchThreadNum; i++)
            {
                m_threads.emplace_back([&] {
                    if (p_opts.m_dspann) // Warning: this code must be run under 4-Byte SizeType
                        p_index->WorkerSingleIndex();
                    else if (p_opts.m_distKV && !p_opts.m_isCoordinator)
                        p_index->WorkerDistKV();
                    else if (p_opts.m_distKV && p_opts.m_isCoordinator){
                        p_index->WorkerTopLayerNode();
                    } else {
                        LOG(Helper::LogLevel::LL_Info, "Unknown Process, Exit\n");
                        exit(0);
                    }
                });
            }

            try {
                zmq::proxy(static_cast<void*>(frontend),
                        static_cast<void*>(backend),
                        nullptr);
            }
            catch (std::exception &e) {}          
            return ErrorCode::Success;
        }

        int BootProgram(const char* storePath) {
            std::shared_ptr<VectorIndex> index;
            if (index->LoadIndex(storePath, index) != ErrorCode::Success) {
                LOG(Helper::LogLevel::LL_Error, "Failed to load index.\n");
                return 1;
            }

            SPANN::Options* opts = nullptr;

            #define DefineVectorValueType(Name, Type) \
                if (index->GetVectorValueType() == VectorValueType::Name) { \
                    opts = ((SPANN::Index<Type>*)index.get())->GetOptions(); \
                    if (true) { \
                        BrokerOn((SPANN::Index<Type>*)(index.get())); \
                    } else { \
                        LOG(Helper::LogLevel::LL_Error, "Error parameters.\n"); \
                    } \
                } \

            #include "inc/Core/DefinitionList.h"
            #undef DefineVectorValueType

            return 0;

        }
    }
}