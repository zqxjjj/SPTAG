#include <iostream>

#include "inc/Core/Common.h"
#include "inc/Core/VectorIndex.h"
#include "inc/Core/SPANN/Index.h"
#include "inc/Helper/SimpleIniReader.h"
#include "inc/Helper/VectorSetReader.h"
#include "inc/Helper/StringConvert.h"
#include "inc/Core/Common/TruthSet.h"

#include "inc/MergePosting/MergePosting.h"

using namespace SPTAG;

// switch between exe and static library by _$(OutputType) 
#ifdef _exe

int main(int argc, char* argv[]) {
	auto ret = MergePosting::BootProgram(argc - 1, argv + 1);
	return ret;
}

#endif