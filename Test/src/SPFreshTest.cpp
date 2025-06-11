// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/Test.h"
#include "inc/Helper/VectorSetReader.h"
#include "inc/Helper/SimpleIniReader.h"
#include "inc/Core/VectorIndex.h"
#include "inc/Core/SPANN/Index.h"
#include "inc/Helper/DiskIO.h"
#include "inc/Core/Common/CommonUtils.h"
#include "inc/Core/Common/QueryResultSet.h"
#include "inc/Core/Common/DistanceUtils.h"
#include <thread>
#include <unordered_set>
#include <ctime>

using namespace SPTAG;

namespace SPFreshTest {
SizeType n = 10000, q = 100;
DimensionType m = 100;
int k = 10;

template <typename T>
std::shared_ptr<SPTAG::VectorIndex> BuildIndex(SPTAG::IndexAlgoType algo, std::shared_ptr<SPTAG::VectorSet>& vec, std::shared_ptr<SPTAG::MetadataSet>& meta, const std::string out)
{
    std::shared_ptr<SPTAG::VectorIndex> vecIndex = SPTAG::VectorIndex::CreateInstance(algo, SPTAG::GetEnumValueType<T>());
    BOOST_CHECK(nullptr != vecIndex);

    std::string configstr =
"[Base]\n\
DistCalcMethod=L2\n\
IndexAlgoType=BKT\n\
ValueType=Int8\n\
Dim=" + std::to_string(m) + "\n\
IndexDirectory=" + out + "\n\
[SelectHead]\n\
isExecute=true\n\
NumberOfThreads=16\n\
SelectThreshold=0\n\
SplitFactor=0\n\
SplitThreshold=0\n\
Ratio=0.2\n\
[BuildHead]\n\
isExecute=true\n\
NumberOfThreads=16\n\
[BuildSSDIndex]\n\
isExecute=true\n\
BuildSsdIndex=true\n\
InternalResultNum=64\n\
SearchInternalResultNum=64\n\
NumberOfThreads=16\n\
PostingPageLimit=4\n\
SearchPostingPageLimit=4\n\
TmpDir=tmpdir\n\
UseFileIO=true\n\
UseSPDKIO=false\n\
UseKV=false\n\
SpdkBatchSize=64\n\
ExcludeHead=false\n\
ResultNum=10\n\
SearchThreadNum=2\n\
Update=true\n\
SteadyState=true\n\
InsertThreadNum=1\n\
AppendThreadNum=1\n\
ReassignThreadNum=0\n\
DisableReassign=false\n\
ReassignK=64\n\
LatencyLimit=50.0\n\
SearchDuringUpdate=true\n\
MergeThreshold=10\n\
Sampling=4\n\
BufferLength=6\n\
InPlace=true\n\
";

    SPTAG::Helper::IniReader iniReader;
    std::shared_ptr<SPTAG::Helper::DiskIO> bufferhandle(new SPTAG::Helper::SimpleBufferIO());
    if (bufferhandle == nullptr || !bufferhandle->Initialize(configstr.data(), std::ios::in, configstr.size())) return nullptr;
    if (SPTAG::ErrorCode::Success != iniReader.LoadIni(bufferhandle)) return nullptr;
    
    std::string sections[] = { "Base", "SelectHead", "BuildHead", "BuildSSDIndex" };
    for (int i = 0; i < 4; i++) {
    auto parameters = iniReader.GetParameters(sections[i].c_str());
    for (auto iter = parameters.begin(); iter != parameters.end(); iter++) {
        vecIndex->SetParameter(iter->first.c_str(), iter->second.c_str(), sections[i].c_str());
    }
    }

    vecIndex->BuildIndex(vec, meta, true, false, false);
    return vecIndex;
}

template <typename T>
void Search(std::shared_ptr<VectorIndex>& vecIndex, std::shared_ptr<VectorSet>& queryset, std::shared_ptr<SPTAG::VectorSet>& vec, std::shared_ptr<SPTAG::VectorSet>& addvec, int k, std::shared_ptr<VectorSet>& truth)
{
    SPTAG::SPANN::Index<T>* p_index = (SPTAG::SPANN::Index<T>*)vecIndex.get();
    //p_index->Initialize();
    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Begin Searching Index...\n");
    std::vector<QueryResult> res(queryset->Count(), QueryResult(nullptr, 64, true));
    auto t1 = std::chrono::high_resolution_clock::now();
    for (SizeType i = 0; i < queryset->Count(); i++)
    {
        res[i].SetTarget(queryset->GetVector(i));
        //vecIndex->SearchIndex(res[i]);
	p_index->GetMemoryIndex()->SearchIndex(res[i]);
        p_index->SearchDiskIndex(res[i], nullptr);

    }
    //p_index->ExitBlockController();
    auto t2 = std::chrono::high_resolution_clock::now();
    std::cout << "Search time: " << (std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count() / (float)(queryset->Count())) << "us" << std::endl;

    float eps = 1e-4f, recall = 0;
    bool deleted;
    int truthDimension = min(k, truth->Dimension());
    for (SizeType i = 0; i < queryset->Count(); i++)
    {
        SizeType* nn = (SizeType*)(truth->GetVector(i));
        for (int j = 0; j < truthDimension; j++)
        {
            std::string truthstr = std::to_string(nn[j]);
            ByteArray truthmeta = ByteArray((std::uint8_t*)(truthstr.c_str()), truthstr.length(), false);
	    float truthdist = 0;
	    if (nn[j] < n) truthdist = vecIndex->ComputeDistance(queryset->GetVector(i), vec->GetVector(nn[j]));
	    else truthdist = vecIndex->ComputeDistance(queryset->GetVector(i), addvec->GetVector(nn[j] - n));
            for (int l = 0; l < k; l++)
            {
		if (nn[j] == res[i].GetResult(l)->VID) {
		    recall += 1.0;
		    break;
		}
		else if (fabs(truthdist - res[i].GetResult(l)->Dist) <= eps * (fabs(truthdist) + eps)) {
                    recall += 1.0;
                    break;
                }
            }
        }
    }
    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Recall %d@%d: %f\n", k, truthDimension, recall / queryset->Count() / truthDimension);
}

template <typename T>
void GenerateData(std::shared_ptr<VectorSet>& vecset, std::shared_ptr<MetadataSet>& metaset, std::shared_ptr<VectorSet>& queryset, std::shared_ptr<VectorSet>& truth, 
    std::shared_ptr<VectorSet>& addvecset, std::shared_ptr<MetadataSet>& addmetaset, std::shared_ptr<VectorSet>& addtruth, std::string distCalcMethod, int k)
{
    if (fileexists("perftest_vector.bin") && fileexists("perftest_meta.bin") && fileexists("perftest_metaidx.bin") && fileexists("perftest_query.bin") && 
        fileexists("perftest_addvector.bin") && fileexists("perftest_addmeta.bin") && fileexists("perftest_addmetaidx.bin")) {
        std::shared_ptr<Helper::ReaderOptions> options(new Helper::ReaderOptions(GetEnumValueType<T>(), m, VectorFileType::DEFAULT));
        auto vectorReader = Helper::VectorSetReader::CreateInstance(options);
        if (ErrorCode::Success != vectorReader->LoadFile("perftest_vector.bin"))
        {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failed to read vector file.\n");
            exit(1);
        }
        vecset = vectorReader->GetVectorSet();

        metaset.reset(new MemMetadataSet("perftest_meta.bin", "perftest_metaidx.bin", vecset->Count() * 2, vecset->Count() * 2, 10));

        if (ErrorCode::Success != vectorReader->LoadFile("perftest_query.bin"))
        {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failed to read query file.\n");
            exit(1);
        }
        queryset = vectorReader->GetVectorSet();

        auto addReader = Helper::VectorSetReader::CreateInstance(options);
        if (ErrorCode::Success != addReader->LoadFile("perftest_addvector.bin"))
        {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failed to read vector file.\n");
            exit(1);
        }
        addvecset = addReader->GetVectorSet();

        addmetaset.reset(new MemMetadataSet("perftest_addmeta.bin", "perftest_addmetaidx.bin", addvecset->Count() * 2, addvecset->Count() * 2, 10));
    }
    else {
        ByteArray vec = ByteArray::Alloc(sizeof(T) * n * m);
        for (SizeType i = 0; i < n; i++) {
            for (DimensionType j = 0; j < m; j++) {
                ((T*)vec.Data())[i * m + j] = (T)COMMON::Utils::rand(127, -127);
            }
        }
        vecset.reset(new BasicVectorSet(vec, GetEnumValueType<T>(), m, n));
        vecset->Save("perftest_vector.bin");

        ByteArray meta = ByteArray::Alloc(n * 6);
        ByteArray metaoffset = ByteArray::Alloc((n + 1) * sizeof(std::uint64_t));
        std::uint64_t offset = 0;
        for (SizeType i = 0; i < n; i++) {
            ((std::uint64_t*)metaoffset.Data())[i] = offset;
            std::string a = std::to_string(i);
            memcpy(meta.Data() + offset, a.c_str(), a.length());
            offset += a.length();
        }
        ((std::uint64_t*)metaoffset.Data())[n] = offset;
        metaset.reset(new MemMetadataSet(meta, metaoffset, n, n * 2, n * 2, 10));
        metaset->SaveMetadata("perftest_meta.bin", "perftest_metaidx.bin");

        ByteArray query = ByteArray::Alloc(sizeof(T) * q * m);
        for (SizeType i = 0; i < q; i++) {
            for (DimensionType j = 0; j < m; j++) {
                ((T*)query.Data())[i * m + j] = (T)COMMON::Utils::rand(127, -127);
            }
        }
        queryset.reset(new BasicVectorSet(query, GetEnumValueType<T>(), m, q));
        queryset->Save("perftest_query.bin");

        for (SizeType i = 0; i < n; i++) {
            for (DimensionType j = 0; j < m; j++) {
                ((T*)vec.Data())[i * m + j] = (T)COMMON::Utils::rand(127, -127);
            }
        }
        addvecset.reset(new BasicVectorSet(vec, GetEnumValueType<T>(), m, n));
        addvecset->Save("perftest_addvector.bin");

        offset = 0;
        for (SizeType i = 0; i < n; i++) {
            ((std::uint64_t*)metaoffset.Data())[i] = offset;
            std::string a = std::to_string(i + n);
            memcpy(meta.Data() + offset, a.c_str(), a.length());
            offset += a.length();
        }
        ((std::uint64_t*)metaoffset.Data())[n] = offset;
        addmetaset.reset(new MemMetadataSet(meta, metaoffset, n, n * 2, n * 2, 10));
        addmetaset->SaveMetadata("perftest_addmeta.bin", "perftest_addmetaidx.bin");
    }

    if (fileexists(("perftest_truth." + distCalcMethod).c_str())) {
        std::shared_ptr<Helper::ReaderOptions> options(new Helper::ReaderOptions(GetEnumValueType<float>(), k, VectorFileType::DEFAULT));
        auto vectorReader = Helper::VectorSetReader::CreateInstance(options);
        if (ErrorCode::Success != vectorReader->LoadFile("perftest_truth." + distCalcMethod))
        {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failed to read truth file.\n");
            exit(1);
        }
        truth = vectorReader->GetVectorSet();
    }
    else {
        omp_set_num_threads(5);

        DistCalcMethod distMethod;
        Helper::Convert::ConvertStringTo(distCalcMethod.c_str(), distMethod);
        if (distMethod == DistCalcMethod::Cosine) {
            std::cout << "Normalize vecset!" << std::endl;
            COMMON::Utils::BatchNormalize((T*)(vecset->GetData()), vecset->Count(), vecset->Dimension(), COMMON::Utils::GetBase<T>(), 5);
        }

        ByteArray tru = ByteArray::Alloc(sizeof(float) * queryset->Count() * k);

#pragma omp parallel for
        for (SizeType i = 0; i < queryset->Count(); ++i)
        {
            SizeType* neighbors = ((SizeType*)tru.Data()) + i * k;

            COMMON::QueryResultSet<T> res((const T*)queryset->GetVector(i), k);
            for (SizeType j = 0; j < vecset->Count(); j++)
            {
                float dist = COMMON::DistanceUtils::ComputeDistance(res.GetTarget(), reinterpret_cast<T*>(vecset->GetVector(j)), queryset->Dimension(), distMethod);
                res.AddPoint(j, dist);
            }
            res.SortResult();
            for (int j = 0; j < k; j++) neighbors[j] = res.GetResult(j)->VID;
        }
        truth.reset(new BasicVectorSet(tru, GetEnumValueType<float>(), k, queryset->Count()));
        truth->Save("perftest_truth." + distCalcMethod);
    }

    if (fileexists(("perftest_addtruth." + distCalcMethod).c_str())) {
        std::shared_ptr<Helper::ReaderOptions> options(new Helper::ReaderOptions(GetEnumValueType<float>(), k, VectorFileType::DEFAULT));
        auto vectorReader = Helper::VectorSetReader::CreateInstance(options);
        if (ErrorCode::Success != vectorReader->LoadFile("perftest_addtruth." + distCalcMethod))
        {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failed to read truth file.\n");
            exit(1);
        }
        addtruth = vectorReader->GetVectorSet();
    }
    else {
        omp_set_num_threads(5);

        DistCalcMethod distMethod;
        Helper::Convert::ConvertStringTo(distCalcMethod.c_str(), distMethod);
        if (distMethod == DistCalcMethod::Cosine) {
            std::cout << "Normalize vecset!" << std::endl;
            COMMON::Utils::BatchNormalize((T*)(vecset->GetData()), vecset->Count(), vecset->Dimension(), COMMON::Utils::GetBase<T>(), 5);
        }

        ByteArray tru = ByteArray::Alloc(sizeof(float) * queryset->Count() * k);

#pragma omp parallel for
        for (SizeType i = 0; i < queryset->Count(); ++i)
        {
            SizeType* neighbors = ((SizeType*)tru.Data()) + i * k;

            COMMON::QueryResultSet<T> res((const T*)queryset->GetVector(i), k);
            for (SizeType j = 0; j < vecset->Count(); j++)
            {
                float dist = COMMON::DistanceUtils::ComputeDistance(res.GetTarget(), reinterpret_cast<T*>(vecset->GetVector(j)), queryset->Dimension(), distMethod);
                res.AddPoint(j, dist);
            }
            for (SizeType j = 0; j < addvecset->Count(); j++)
            {
                float dist = COMMON::DistanceUtils::ComputeDistance(res.GetTarget(), reinterpret_cast<T*>(addvecset->GetVector(j)), queryset->Dimension(), distMethod);
                res.AddPoint(j + n, dist);
            }
            res.SortResult();
            for (int j = 0; j < k; j++) neighbors[j] = res.GetResult(j)->VID;
        }
        addtruth.reset(new BasicVectorSet(tru, GetEnumValueType<float>(), k, queryset->Count()));
        addtruth->Save("perftest_addtruth." + distCalcMethod);
    }
}

template <typename T>
void CTest(SPTAG::IndexAlgoType algo, std::string distCalcMethod)
{
    std::shared_ptr<VectorSet> vecset, queryset, truth, addset, addtruth;
    std::shared_ptr<MetadataSet> metaset, addmetaset;
    GenerateData<T>(vecset, metaset, queryset, truth, addset, addmetaset, addtruth, distCalcMethod, k); 

    std::shared_ptr<SPTAG::VectorIndex> vecIndex = BuildIndex<T>(algo, vecset, metaset, "testindices");
    Search<T>(vecIndex, queryset, vecset, addset, k, truth);
    vecIndex->SaveIndex("testindices");
    vecIndex.reset();
    if (SPTAG::VectorIndex::LoadIndex("testindices", vecIndex) != ErrorCode::Success) return;
    BOOST_CHECK(nullptr != vecIndex);
    Search<T>(vecIndex, queryset, vecset, addset, k, truth);

    vecIndex->AddIndex(addset, addmetaset, true, false);
    Search<T>(vecIndex, queryset, vecset, addset, k, addtruth);
    vecIndex->SaveIndex("testindices");
    vecIndex.reset();
    if (SPTAG::VectorIndex::LoadIndex("testindices", vecIndex) != ErrorCode::Success) return;
    BOOST_CHECK(nullptr != vecIndex);
    Search<T>(vecIndex, queryset, vecset, addset, k, addtruth);
}
}

BOOST_AUTO_TEST_SUITE(SPFreshTest)


BOOST_AUTO_TEST_CASE(FlowTest)
{
	SPFreshTest::CTest<int8_t>(SPTAG::IndexAlgoType::SPANN, "L2");
}

BOOST_AUTO_TEST_SUITE_END()
