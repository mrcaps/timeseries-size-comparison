/*
 * data.hpp
 * Interface to {ts,vs} and mergemap files
 */

#ifndef DATA_HPP_
#define DATA_HPP_

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>

using namespace std;

/**
 * @param start are we starting or ending?
 */
void timeit(bool start, double nthings=0) {
	static time_t tstart;
	static bool started=false;

	if (start == started) {
		cerr << "Timer not flipping state!" << endl;
	}

	if (start) {
		time(&tstart);
		started = true;
	} else {
		time_t tend;
		time(&tend);
		started = false;
		cerr << "ops/sec " << nthings / difftime(tend, tstart) << endl;
	}
}

enum StreamT { TS, VS };

/**
 * Data source - flat binary files, multiple value streams per timestamp stream
 */
class DataMulti {
private:
	void init() {
		DIR *dir;
		struct dirent *ent;
		if (NULL != (dir = opendir(tsdir))) {
			while (NULL != (ent = readdir(dir))) {
				string name = string(ent->d_name);
				if (name.length() > 3) {
					name.erase(name.find("."));
					//assume mergemap is complete.
					// just check for entries not in mergemap
					if (merge.find(name) == merge.end() && (name != "fmerge")) {
						cerr << "Warning: could not find ts entry for " << name << endl;
					}
				}
			}
			closedir(dir);
		} else {
			cerr << "Coudln't open directory " << tsdir << endl;
		}
	}

public:
	map<string, set<string> > merge;
	const char* tsdir;
	const char* vsdir;
	DataMulti(const char *_tsdir, const char *_vsdir,
			map<string, set<string> > _merge) :
				merge(_merge), tsdir(_tsdir), vsdir(_vsdir) {

		init();
	}

	map<string, set<string> >::const_iterator begin() {
		return merge.begin();
	}
	map<string, set<string> >::const_iterator end() {
		return merge.end();
	}

	string get_name(string item, StreamT type) {
		return string((type == TS) ? tsdir : vsdir) + string("/") + item +
				string(type == TS ? ".ts" : ".vs");
	}
};

/**
 * Data source - flat binary files, one value stream per timestamp stream
 */
class Data {
private:
	const char *tsdir;
	const char *vsdir;
	set<string> diritems;

	bool isMulti;
	DataMulti *dmulti;
	map<string, string> *revmap;

	void init() {
		DIR *dir;
		struct dirent *ent;
		if (NULL != (dir = opendir(tsdir))) {
			while (NULL != (ent = readdir(dir))) {
				const char* name = ent->d_name;
				if (name[0] != '\0' && name[1] != '\0' && name[2] != '\0') {
					//XXX: we should really copy.
					char *dloc = const_cast<char*>(strchr(name, '.'));
					*dloc = '\0';
					diritems.insert(string(name));
				}
			}
			closedir(dir);
		} else {
			cout << "Coudln't open directory " << tsdir << endl;
		}
	}

public:
	Data(const char* _tsdir, const char* _vsdir) :
		tsdir(_tsdir), vsdir(_vsdir), isMulti(false), dmulti(NULL), revmap(NULL) {

		init();
	}

	/**
	 * Expose a DataMulti as a Data. Caller is responsible for dm.
	 */
	Data(DataMulti dm) : tsdir(dm.tsdir), vsdir(dm.vsdir), isMulti(true), dmulti(&dm) {
		revmap = new map<string, string>;
		for (map<string, set<string> >::const_iterator it = dm.begin(); it != dm.end(); ++it) {
			for (set<string>::const_iterator sit = (*it).second.begin(); sit != (*it).second.end(); ++sit) {
				(*revmap)[*sit] = (*it).first;
				diritems.insert(*sit);
			}
		}
	}

	set<string>::const_iterator begin() {
		return diritems.begin();
	}
	set<string>::const_iterator end() {
		return diritems.end();
	}

	string get_name(string item, StreamT type) {
		if (isMulti && type == TS) {
			return string(tsdir) + string("/") + (*revmap)[item] + string(".ts");
		} else {
			return string(type == TS ? tsdir : vsdir) + string("/") + item +
					string(type == TS ? ".ts" : ".vs");
		}
	}
};

map<string, set<string> > getMergeMap(const char* fname) {
	map<string, set<string> > themap;
	ifstream fin(fname, ios::in);

	if (!fin) {
		cerr << "coudln't find map input file " << fname << endl;
	}

	int ldx = 0;
	string tskey;
	string vskey;
	while (fin.good()) {
		if (ldx % 2 == 0) {
			fin >> tskey;
		} else {
			fin >> vskey;
			unsigned pos = vskey.rfind("/");
			if (pos != string::npos) {
				vskey.erase(0, pos + 1);
			}
			pos = vskey.find(".");
			if (pos != string::npos) {
				vskey.erase(vskey.find("."));
			}
			if (themap.find(tskey) == themap.end()) {
				themap[tskey] = set<string>();
			}
			themap[tskey].insert(vskey);
		}
		if (!fin.good()) {
			break;
		}

		++ldx;
	}

	fin.close();

	return themap;
}

void test_datamulti() {
	DataMulti data("../testdata-multi/ts.merge", "../testdata-multi/vs",
			getMergeMap("../testdata-multi/ts.merge/fmerge.sorted.txt"));

	for (map<string, set<string> >::const_iterator it = data.begin(); it != data.end(); ++it) {
		for (set<string>::const_iterator sit = (*it).second.begin(); sit != (*it).second.end(); ++sit) {
			cerr << (*it).first << " " << (*sit) << endl;
		}
	}
}

void test_mergemap() {
	getMergeMap("../testdata-multi/ts.merge/fmerge.sorted.txt");
}

#endif /* DATA_HPP_ */
