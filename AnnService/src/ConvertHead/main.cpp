#include <cstdint>
#include <iostream>

#include "inc/Core/Common.h"
#include "inc/Helper/StringConvert.h"
#include "inc/Helper/ArgumentsParser.h"

using namespace SPTAG;

class ConvertOptions : public Helper::ArgumentsParser
{
public:
    ConvertOptions()
    {
        AddRequiredOption(m_inputDir, "-id", "--inputDir", "Input dir");
		AddRequiredOption(m_outputDir, "-od", "--outputDir", "Output dir");
		AddRequiredOption(m_inputValueType, "-v", "--vectortype", "Input vector data type. Default is float.");
		AddOptionalOption(m_vectorFile, "-vf", "--vectorFile", "vectorFile, default is vectors.bin");
		AddOptionalOption(m_treeFile, "-tf", "--treeFile", "treeFile, default is tree.bin");
		AddOptionalOption(m_graphFile, "-gf", "--graphFile", "treeFile, default is graph.bin");
		AddOptionalOption(m_deleteFile, "-df", "--deleteFile", "treeFile, default is deletes.bin");
		AddOptionalOption(m_quantizerFile, "-qf", "--quantizerFile", "quantizerFile, default is quantizer.bin");
        
    }

    ~ConvertOptions() {}

    std::string m_inputDir;
	std::string m_outputDir;
	SPTAG::VectorValueType m_inputValueType;
	std::string m_vectorFile = "vectors.bin";
	std::string m_treeFile = "tree.bin";
	std::string m_graphFile = "graph.bin";
	std::string m_deleteFile = "deletes.bin";
	std::string m_quantizerFile = "quantizer.bin";
    
} options;

template <typename T>
ErrorCode ConvertHeadInternal()
{
	options.m_inputDir += FolderSep;
	options.m_outputDir += FolderSep;

	LOG(Helper::LogLevel::LL_Info, "Converting 4_byte to 8_byte\n");

	std::string folderPath(options.m_outputDir);
    if (!folderPath.empty() && *(folderPath.rbegin()) != FolderSep)
    {
        folderPath += FolderSep;
    }
    if (!direxists(folderPath.c_str()))
    {
        mkdir(folderPath.c_str());
    }

    // Data points
	{
		LOG(Helper::LogLevel::LL_Info, "Converting m_pSamples\n");
		char* data = nullptr;
		int rows;
		DimensionType mycols;
		auto pInput = SPTAG::f_createIO();
		std::string filename = options.m_inputDir + options.m_vectorFile;

		if (pInput == nullptr || !pInput->Initialize(filename.c_str(), std::ios::binary | std::ios::in)) {
			LOG(Helper::LogLevel::LL_Error, "Failed open file %s\n", filename.c_str());
			throw std::runtime_error("Failed to open file");
		}

		IOBINARY(pInput, ReadBinary, sizeof(int), (char*)&(rows));
		IOBINARY(pInput, ReadBinary, sizeof(DimensionType), (char*)&mycols);

		DimensionType cols = mycols * sizeof(T);
		data = (char*)ALIGN_ALLOC(((size_t)rows) * cols);

		for (int i = 0; i < rows; i++) {
			IOBINARY(pInput, ReadBinary, sizeof(T) * mycols, (char*)((T*)(data + ((size_t)i) * cols)));
		}
		LOG(Helper::LogLevel::LL_Info, "Load %s (%d,%d) Finish!\n", filename.c_str(), rows, mycols);


		auto ptr_out = SPTAG::f_createIO();
		filename = options.m_outputDir + options.m_vectorFile;

		if (ptr_out == nullptr || !ptr_out->Initialize(filename.c_str(), std::ios::binary | std::ios::out)) {
			LOG(Helper::LogLevel::LL_Error, "Failed open file %s\n", filename.c_str());
			throw std::runtime_error("Failed to open file");
		}
		
		int64_t CR = rows;

		IOBINARY(ptr_out, WriteBinary, sizeof(int64_t), (char*)&CR);
		IOBINARY(ptr_out, WriteBinary, sizeof(DimensionType), (char*)&mycols);
		for (int64_t i = 0; i < CR; i++) {
			IOBINARY(ptr_out, WriteBinary, sizeof(T) * mycols, (char*)((T*)(data + ((size_t)i) * cols)));
		}

		ALIGN_FREE(data);

		LOG(Helper::LogLevel::LL_Info, "Save %s (%lld,%d) Finish!\n", filename.c_str(), CR, mycols);
	}

	// BKT structures. 
	{
		struct BKTNode_Before
        {
            int centerid;
            int childStart;
            int childEnd;

            BKTNode_Before(int cid = -1) : centerid(cid), childStart(-1), childEnd(-1) {}
        };

		struct BKTNode_After
        {
            int64_t centerid;
            int64_t childStart;
            int64_t childEnd;

            BKTNode_After(int cid = -1) : centerid(cid), childStart(-1), childEnd(-1) {}
        };

		std::vector<int> m_pTreeStart;
        std::vector<BKTNode_Before> m_pTreeRoots;
		int m_iTreeNumber;

		LOG(Helper::LogLevel::LL_Info, "Converting m_pTrees\n");

		auto p_input = SPTAG::f_createIO();
		std::string filename = options.m_inputDir + options.m_treeFile;

		if (p_input == nullptr || !p_input->Initialize(filename.c_str(), std::ios::binary | std::ios::in)) {
			LOG(Helper::LogLevel::LL_Error, "Failed open file %s\n", filename.c_str());
			throw std::runtime_error("Failed to open file");
		}

		IOBINARY(p_input, ReadBinary, sizeof(m_iTreeNumber), (char*)&m_iTreeNumber);
		m_pTreeStart.resize(m_iTreeNumber);
		IOBINARY(p_input, ReadBinary, sizeof(int) * m_iTreeNumber, (char*)m_pTreeStart.data());

		// SizeType treeNodeSize;
		int treeNodeSize;
		IOBINARY(p_input, ReadBinary, sizeof(treeNodeSize), (char*)&treeNodeSize);
		m_pTreeRoots.resize(treeNodeSize);
		IOBINARY(p_input, ReadBinary, sizeof(BKTNode_Before) * treeNodeSize, (char*)m_pTreeRoots.data());

		if (m_pTreeRoots.size() > 0 && m_pTreeRoots.back().centerid != -1) m_pTreeRoots.emplace_back(-1);
		LOG(Helper::LogLevel::LL_Info, "Load %s (%d,%d) Finish!\n", filename.c_str(), m_iTreeNumber, treeNodeSize);

		std::vector<BKTNode_After> m_pTreeRoots_After(treeNodeSize);
		for (int i = 0; i < treeNodeSize; i++) {
			m_pTreeRoots_After[i].centerid = m_pTreeRoots[i].centerid;
			m_pTreeRoots_After[i].childStart = m_pTreeRoots[i].childStart;
			m_pTreeRoots_After[i].childEnd = m_pTreeRoots[i].childEnd;
		}

		auto p_out = SPTAG::f_createIO();
		filename = options.m_outputDir + options.m_treeFile;

		if (p_out == nullptr || !p_out->Initialize(filename.c_str(), std::ios::binary | std::ios::out)) {
			LOG(Helper::LogLevel::LL_Error, "Failed open file %s\n", filename.c_str());
			throw std::runtime_error("Failed to open file");
		}
		std::vector<int64_t> m_pTreeStart_After(m_iTreeNumber);
		
		for (int i = 0; i < m_iTreeNumber; i++) {
			m_pTreeStart_After[i] = m_pTreeStart[i];
		}

		IOBINARY(p_out, WriteBinary, sizeof(m_iTreeNumber), (char*)&m_iTreeNumber);
		IOBINARY(p_out, WriteBinary, sizeof(int64_t) * m_iTreeNumber, (char*)m_pTreeStart_After.data());
		int64_t treeNodeSize_After = m_pTreeRoots_After.size();
		IOBINARY(p_out, WriteBinary, sizeof(treeNodeSize_After), (char*)&treeNodeSize_After);
		IOBINARY(p_out, WriteBinary, sizeof(BKTNode_After) * treeNodeSize_After, (char*)m_pTreeRoots_After.data());
		LOG(Helper::LogLevel::LL_Info, "Save %s (%d,%lld) Finish!\n", filename.c_str(), m_iTreeNumber, treeNodeSize_After);
	}


	// Graph structure
	{
		LOG(Helper::LogLevel::LL_Info, "Converting m_pGraph\n");
		char* data = nullptr;
		int rows;
		DimensionType mycols;
		auto pInput = SPTAG::f_createIO();
		std::string filename = options.m_inputDir + options.m_graphFile;

		if (pInput == nullptr || !pInput->Initialize(filename.c_str(), std::ios::binary | std::ios::in)) {
			LOG(Helper::LogLevel::LL_Error, "Failed open file %s\n", filename.c_str());
			throw std::runtime_error("Failed to open file");
		}

		IOBINARY(pInput, ReadBinary, sizeof(int), (char*)&(rows));
		IOBINARY(pInput, ReadBinary, sizeof(DimensionType), (char*)&mycols);

		DimensionType cols = mycols * sizeof(int);
		data = (char*)ALIGN_ALLOC(((size_t)rows) * cols);

		for (int i = 0; i < rows; i++) {
			IOBINARY(pInput, ReadBinary, sizeof(int) * mycols, (char*)((int*)(data + ((size_t)i) * cols)));
		}
		LOG(Helper::LogLevel::LL_Info, "Load %s (%d,%d) Finish!\n", filename, rows, mycols);


		auto ptr_out = SPTAG::f_createIO();
		filename = options.m_outputDir + options.m_graphFile;

		if (ptr_out == nullptr || !ptr_out->Initialize(filename.c_str(), std::ios::binary | std::ios::out)) {
			LOG(Helper::LogLevel::LL_Error, "Failed open file %s\n", filename.c_str());
			throw std::runtime_error("Failed to open file");
		}
		
		int64_t CR = rows;

		IOBINARY(ptr_out, WriteBinary, sizeof(int64_t), (char*)&CR);
		IOBINARY(ptr_out, WriteBinary, sizeof(DimensionType), (char*)&mycols);
		for (int64_t i = 0; i < CR; i++) {
			for (int j = 0; j < mycols; j++) {
				int64_t neighborID = *((int*)(data + ((size_t)i) * j * sizeof(int)));
				IOBINARY(ptr_out, WriteBinary, sizeof(int64_t), (char*)&neighborID);
			}
		}

		ALIGN_FREE(data);

		LOG(Helper::LogLevel::LL_Info, "Save %s (%lld,%d) Finish!\n", filename.c_str(), CR, mycols);
	}

	// Deletes ID
	{
		LOG(Helper::LogLevel::LL_Info, "Converting m_deletedID\n");
		char* data = nullptr;
		int rows;
		DimensionType mycols;
		int deleted;
		auto pInput = SPTAG::f_createIO();
		std::string filename = options.m_inputDir + options.m_deleteFile;

		if (pInput == nullptr || !pInput->Initialize(filename.c_str(), std::ios::binary | std::ios::in)) {
			LOG(Helper::LogLevel::LL_Error, "Failed open file %s\n", filename.c_str());
			throw std::runtime_error("Failed to open file");
		}

		IOBINARY(pInput, ReadBinary, sizeof(int), (char*)&(deleted));

		IOBINARY(pInput, ReadBinary, sizeof(int), (char*)&(rows));
		IOBINARY(pInput, ReadBinary, sizeof(DimensionType), (char*)&mycols);

		DimensionType cols = mycols * sizeof(int8_t);
		// LOG(Helper::LogLevel::LL_Info, "Load %lld bytes block\n", ((size_t)rows) * cols);
		data = (char*)ALIGN_ALLOC(((size_t)rows) * cols);

		for (int i = 0; i < rows; i++) {
			IOBINARY(pInput, ReadBinary, sizeof(int8_t) * mycols, (char*)((int8_t*)(data + ((size_t)i) * cols)));
		}
		LOG(Helper::LogLevel::LL_Info, "Load %s (%d,%d) Finish!\n", filename, rows, mycols);


		auto ptr_out = SPTAG::f_createIO();
		filename = options.m_outputDir + options.m_deleteFile;

		if (ptr_out == nullptr || !ptr_out->Initialize(filename.c_str(), std::ios::binary | std::ios::out)) {
			LOG(Helper::LogLevel::LL_Error, "Failed open file %s\n", filename.c_str());
			throw std::runtime_error("Failed to open file");
		}
		
		int64_t CR = rows;
		int64_t deleted_after = deleted;

		IOBINARY(ptr_out, WriteBinary, sizeof(int64_t), (char*)&deleted_after);
		IOBINARY(ptr_out, WriteBinary, sizeof(int64_t), (char*)&CR);
		IOBINARY(ptr_out, WriteBinary, sizeof(DimensionType), (char*)&mycols);
		for (int64_t i = 0; i < CR; i++) {
			IOBINARY(ptr_out, WriteBinary, sizeof(int8_t) * mycols, (char*)((int8_t*)(data + ((size_t)i) * cols)));
		}

		ALIGN_FREE(data);

		LOG(Helper::LogLevel::LL_Info, "Save %s (%lld,%d) Finish!\n", filename.c_str(), CR, mycols);
	}


	// Quantizer
	{
		LOG(Helper::LogLevel::LL_Info, "Converting Quantizer (Actually we didn't change it, we just copy it)\n");
		std::string filename = options.m_inputDir + options.m_quantizerFile;
		std::string filename_output = options.m_outputDir + options.m_quantizerFile;

		
		int status = system(("cp "+ filename + " " + filename_output).c_str());
		if (status == 0) {
			LOG(Helper::LogLevel::LL_Info, "Quantizer File Copied\n");
		} else {
			LOG(Helper::LogLevel::LL_Info, "Error in QuantizerFile Copy, Exit\n");
			exit(0);
		}
	}

	// Quantizer
	{
		LOG(Helper::LogLevel::LL_Info, "Converting Config File: Indexloader.ini (Actually we didn't change it, we just copy it)\n");
		std::string filename = options.m_inputDir + "indexloader.ini";
		std::string filename_output = options.m_outputDir + "indexloader.ini";

		
		int status = system(("cp "+ filename + " " + filename_output).c_str());
		if (status == 0) {
			LOG(Helper::LogLevel::LL_Info, "Config File Copied\n");
		} else {
			LOG(Helper::LogLevel::LL_Info, "Error in ConfigFile Copy, Exit\n");
			exit(0);
		}
	}

	return ErrorCode::Success;
}



int main(int argc, char* argv[]) {
	if (!options.Parse(argc - 1, argv + 1))
    {
        exit(1);
    }

	if (argc < 3)
	{
		LOG(Helper::LogLevel::LL_Error,
			"ConvertHead requires head dir path and output path\n");
		exit(-1);
	}
	switch (options.m_inputValueType) {
        case SPTAG::VectorValueType::Float:
            ConvertHeadInternal<float>();
            break;
        case SPTAG::VectorValueType::Int16:
            ConvertHeadInternal<std::int16_t>();
            break;
        case SPTAG::VectorValueType::Int8:
            ConvertHeadInternal<std::int8_t>();
            break;
        case SPTAG::VectorValueType::UInt8:
            ConvertHeadInternal<std::uint8_t>();
            break;
        default:
            LOG(Helper::LogLevel::LL_Error, "Error data type!\n");
        }
	return 0;
}

