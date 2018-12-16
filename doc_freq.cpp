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

struct data
{
	string word;
	int freq;
};
typedef data data;


void traverseDirectory(string dirName, vector<string>* threadFileNames);
unordered_map<string,bool> createStopWordIndex(char* stopWordFile);
bool compare(data d1, data d2);
vector<pair<string,int> > getKMaxFreqWords(unordered_map<string,int> finalWordIndex, int k);
void processFile(string file_name,unordered_map<string,int>* threadHashMap,unordered_map<string,bool> stopWordIndex);
void printThreadMap(unordered_map<string,int> m, int threadId,int final);
void test(unordered_map<string,int> m);

int main(int argc, char* argv[])
{
	double stIOtime,endIOtime,stMergeTime,endMergeTime;
	stIOtime = omp_get_wtime();
	int numCores = atoi(argv[1]);
	int numThreadsToReadFiles = 2*numCores; // check this
	DIR* ds;
	int i,j;
	struct dirent* dir;
	char* rootDir = argv[2];
	int k = atoi(argv[3]);
	char* stopWordFile = (char*)malloc(20);
	strcpy(stopWordFile,"stopwords.txt");
	unordered_map<string,bool> stopWordIndex = createStopWordIndex(stopWordFile);
	ds = opendir(rootDir);
	if(ds == NULL)
	{
		printf("directory %s is not opened\n",rootDir);
		return 0;
	}
	vector<string> directories;
	while((dir = readdir(ds)) != NULL)
	{
		if(strcmp(dir->d_name, ".") != 0 && strcmp(dir->d_name, "..") != 0)
		{
			directories.push_back(string(rootDir) + string(dir->d_name));
			// cout << directories[i] << "\n";
		}
	}
	// printf("done getting dir\n");
	if(numThreadsToReadFiles > directories.size())
	{
		numThreadsToReadFiles = directories.size();
	}
	vector<vector<string>* > innerFileNames;
	for(j = 0; j < numThreadsToReadFiles; j++)
	{
		vector<string>* vec = new vector<string>;
		innerFileNames.push_back(vec);
	}
	#pragma omp parallel for schedule(dynamic) num_threads(numThreadsToReadFiles)
	for(i = 0; i < directories.size(); i++)
	{
		int threadRank = omp_get_thread_num();
		traverseDirectory(directories[i],innerFileNames[threadRank]);
	}
	// for(i = 0; i < innerFileNames.size(); i++)
	// {
	// 	printf("thread %d\n",i);
	// 	for(j = 0; j < (*innerFileNames[i]).size(); j++)
	// 	{
	// 		printf("%s\n",(*(innerFileNames[i]))[j].c_str());
	// 	}
	// }
	// got list of all files
	int numFiles = 0;
	vector<string> allFileNames;
	for(i = 0; i < innerFileNames.size(); i++)
	{
		allFileNames.insert(allFileNames.end(),(*(innerFileNames[i])).begin(),(*(innerFileNames[i])).end());
	}
	for(i = 0; i < innerFileNames.size(); i++)
	{
		delete(innerFileNames[i]);
	}
	numFiles = allFileNames.size();
	int numThreadsToCreateIndex = 2*numCores;
	vector<unordered_map<string,int>* > threadMaps;
	threadMaps.resize(numThreadsToCreateIndex);	
	for(i = 0; i < numThreadsToCreateIndex; i++)
	{
		threadMaps[i] = new unordered_map<string,int>;
	}

	#pragma omp parallel for schedule(dynamic) num_threads(numThreadsToCreateIndex)
	for(i = 0; i < numFiles; i++)
	{
		int threadRank = omp_get_thread_num();
		processFile(allFileNames[i],threadMaps[threadRank],stopWordIndex);
	}

	endIOtime = omp_get_wtime();
	stMergeTime = omp_get_wtime();
	// #pragma omp parallel num_threads(numThreadsToCreateIndex)
	// {
	// 	int threadRank = omp_get_thread_num();
	// 	printThreadMap(*(threadMaps[threadRank]),threadRank,0);
	// }
	int height_tree = (int)log2(numThreadsToCreateIndex);
	int level = 1;
	while(level <= height_tree)
	{
		numThreadsToCreateIndex /= 2;
		// omp_set_num_threads(numThreadsToCreateIndex);
		#pragma omp parallel num_threads(numThreadsToCreateIndex)
		{
			int threadRank = omp_get_thread_num();
			int firsti = (int)pow(2,level)*threadRank;
			int secondi = firsti + (int)pow(2,level - 1);
			unordered_map<string,int>::iterator secondMap_itr;
			for(secondMap_itr = (*threadMaps[secondi]).begin(); secondMap_itr != (*threadMaps[secondi]).end(); secondMap_itr++)
			{
				(*threadMaps[firsti])[secondMap_itr->first] += secondMap_itr->second;
			} 
		}
		level++;
	}
	// printThreadMap(*(threadMaps[0]),0,1);
	// test(*(threadMaps[0]));
	endMergeTime = omp_get_wtime();
	vector<pair<string,int> > kmax = getKMaxFreqWords(*(threadMaps[0]),k);
	printf("final words\n");
	for(i = 0; i < k; i++)
	{
		cout << kmax[i].first << ":" << kmax[i].second << "\n";
	}
	printf("IO including thread hash map creation: %lf\n",(endIOtime - stIOtime));
	printf("Time taken to create final merged hash map: %lf\n",(endMergeTime - stMergeTime));
	printf("Total time taken: %lf\n",(endMergeTime + endIOtime - stIOtime - stMergeTime));
	return 0;	
}

void test(unordered_map<string,int> m)
{
	vector<pair<int, string> > wordMap;
	unordered_map<string,int>::iterator itr;
	for(itr = m.begin(); itr != m.end(); itr++)
	{
		wordMap.push_back(make_pair(itr->second,itr->first));
	}
	sort(wordMap.begin(), wordMap.end());
	for(int i = 0; i < wordMap.size(); i++)
	{
		cout << wordMap[i].second << "::" << wordMap[i].first << "\n";
	}
}

bool compare(data d1, data d2)
{
	return d1.freq > d2.freq;
}

vector<pair<string,int> > getKMaxFreqWords(unordered_map<string,int> finalWordIndex, int k)
{
	unordered_map<string,int>::iterator map_itr;
	priority_queue<data,vector<data>,function<bool(data,data)> > pq(compare);
	for(map_itr = finalWordIndex.begin(); map_itr != finalWordIndex.end(); map_itr++)
	{
		data d1;
		d1.word = map_itr->first;
		d1.freq = map_itr->second;
		if(pq.size() < k)
		{
			pq.push(d1);
		}
		else
		{
			if(pq.top().freq < d1.freq)
			{
				pq.pop();
				pq.push(d1);
			}
		}
	}
	vector<pair<string,int> > maxFreqWords;
	while(pq.size() > 0)
	{
		data d1 = pq.top();
		maxFreqWords.push_back(make_pair(d1.word,d1.freq));
		pq.pop();
	}
	return maxFreqWords;
}

void printThreadMap(unordered_map<string,int> m, int threadId,int final)
{
	unordered_map<string,int>::iterator map_itr;
	string file_name = "localMap_" + to_string(threadId);
	if(final)
	{
		file_name += "_final";
	}
	ofstream fp(file_name,ios::out);
	fp <<"Thread:";
	fp << threadId;
	fp << "\n";		
	fp << "word:freq\n";
	for(map_itr = m.begin(); map_itr != m.end(); map_itr++)
	{
		fp << map_itr->first;
		fp << ":";
		fp << map_itr->second;
		fp << "\n";
	}
	fp.close();
}

void traverseDirectory(string dirName, vector<string>* threadFileNames)
{
	struct stat sb;
	if(stat(dirName.c_str(),&sb) == 0 && S_ISDIR(sb.st_mode))
	{
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
				traverseDirectory(subDirName,threadFileNames);
			}
		}
	}
	else // file
	{
		(*threadFileNames).push_back(dirName);
	}
}

unordered_map<string,bool> createStopWordIndex(char* stopWordFile)
{
	unordered_map<string,bool> stopWordIndex;
	fstream stfp;
	stfp.open(stopWordFile);
	char stopWord[100];
	while(stfp >> stopWord)
	{
		string word = string(stopWord);
		stopWordIndex[word] = true;
	}
	stfp.close();
	return stopWordIndex;
}

void processFile(string file_name,unordered_map<string,int>* threadHashMap,unordered_map<string,bool> stopWordIndex)
{
	unordered_map<string,bool> localWordExistenceMap;
	fstream doc;
	doc.open(file_name.c_str());
	char temp_word[512];
	string word;
	bool printed = false;
	while(doc >> temp_word)
	{
		char* rest = temp_word;
		// if(!printed) printf("file_name:%s\n",file_name.c_str());
		// printed = true;
		// printf("%s\n",temp_word);
		char* token = strtok_r(rest,"\n ,~-.:;?/!\"",&rest);
		while(token != NULL)
		{
			word = string(token);
			// if(strcmp(temp_word,"seven-year") == 0)
			// {
			// 	printf("file_name:%s,%s\n",file_name.c_str(),word.c_str());
			// }
			transform(word.begin(),word.end(),word.begin(),::tolower);
			if(stopWordIndex[word])
			{
				token = strtok_r(rest,"\n ,~-.:;?/!\"",&rest);
				continue;
			}

			if(!localWordExistenceMap[word])
			{
				localWordExistenceMap[word] = true;
				(*threadHashMap)[word]++;
			}
			token = strtok_r(rest,"\n ,~-.:;?/!\"",&rest);
		}
	}
	doc.close();
}