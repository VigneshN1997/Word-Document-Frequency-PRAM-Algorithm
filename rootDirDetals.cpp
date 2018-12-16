#include <cstdio>
#include <iostream>
#include <dirent.h>
#include <omp.h>
#include <bits/stdc++.h>
#include <cmath>
#include <sys/stat.h>
#include <cstring>
#include <queue>
#include <cstdlib>
#include <string>
#include <ctime>
using namespace std;

void traverseDirectory(string dirName);

long int maxDepth = 0;
long int sumAllDepth = 0;
long int currDepth = 0;
long int totalFiles = 0;
long int totalNumDir = 0;
long int sumBranchingFactors = 0;

int main(int argc, char* argv[])
{
	char* dirName = argv[1];
	
	traverseDirectory(dirName);

	long int avgBranchingFactor = sumBranchingFactors / totalNumDir;
	long int avgDepth = sumAllDepth / totalFiles;
	printf("Max Depth:%ld\n",maxDepth);
	printf("Avg Depth:%ld\n",avgDepth);
	printf("Avg Branching factor:%ld\n",avgBranchingFactor);
	printf("Total Files:%ld\n",totalFiles);
	// printf("sumdepths:%d sumBranchingFactors:%d totalNumDir:%d\n",sumAllDepth,sumBranchingFactors,totalNumDir);

	return 0;
}

void traverseDirectory(string dirName)
{
	struct stat sb;
	if(stat(dirName.c_str(),&sb) == 0 && S_ISDIR(sb.st_mode))
	{
		totalNumDir++;
		DIR* ds = opendir(dirName.c_str());
		if(ds == NULL)
		{
			return;
		}
		struct dirent* dir;
		while((dir = readdir(ds)) != NULL)
		{
			if(strcmp(dir->d_name, ".") != 0 && strcmp(dir->d_name, "..") != 0)
			{
				string subDirName = dirName + "/" + string(dir->d_name);
				currDepth++;
				traverseDirectory(subDirName);
				currDepth--;
				sumBranchingFactors++;
			}
		}
	}
	else // file
	{
		// (*threadFileNames).push_back(dirName);
		sumAllDepth += currDepth;
		totalFiles++;
		if(currDepth > maxDepth)
		{
			maxDepth = currDepth;
		}
	}
}
