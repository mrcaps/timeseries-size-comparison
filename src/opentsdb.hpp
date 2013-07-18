/*
 * opentsdb.hpp
 * OpenTSDB import utils.
 *
 */

#ifndef OPENTSDB_HPP_
#define OPENTSDB_HPP_

#include "data.hpp"

using namespace std;

/**
 * @brief Collect opentsdb metric names to the given output stream
 */
void print_opentsdb_metrics(Data data, ostream &fout) {
	stringstream ss;
	int metricdx = 0;
	for (set<string>::const_iterator it = data.begin(); it != data.end(); ++it) {
		++metricdx;
		ss << "m" << metricdx << " ";
	}
	fout << ss.str() << endl;
}

/**
 * @brief Collect opentsdb metric names to the given output stream
 *  for metrics used with print_opentsdb_inserts_multi
 */
void print_opentsdb_metrics_multi(DataMulti data, ostream &fout) {
	stringstream ss;
	int metricdx = 0;
	for (map<string, set<string> >::const_iterator it = data.begin(); it != data.end(); ++it) {
		++metricdx;
		ss << "m" << metricdx << " ";
	}
	fout << ss.str() << endl;
}

/**
 * @brief Write opentsdb stream to stdout
 * the keys are simply increasing values for a metric name, prefixed by "m"
 */
void print_opentsdb_inserts(Data data, int twidth, int vwidth) {
	char tbuf[twidth];
	char vbuf[vwidth];

	char metricname[1024];
	char insertbuf[4096];
	int metricdx = 0;

	for (set<string>::const_iterator it = data.begin(); it != data.end(); ++it) {
		long ninserts = 0;
		long nolder = 0;

		++metricdx;
		sprintf(metricname, "m%d", metricdx);

		timeit(true);

		//assume existence of both ts and vs. amd that they're the same length
		string tsname = data.get_name(*it, TS);
		string vsname = data.get_name(*it, VS);
		ifstream ts(tsname.c_str(), ios::binary | ios::in);
		ifstream vs(vsname.c_str(), ios::binary | ios::in);
		cerr << "inserting " << *it << endl;

		if (!ts.good()) {
			cerr << "could not find timestamp file " << tsname << endl;
			break;
		}
		if (!vs.good()) {
			cerr << "could not find value file " << vsname << endl;
		}

		int oldts = 0;
		while (ts.good()) {
			if (!vs.good()) {
				cerr << "values were not the same length as timestamps!" << endl;
				break;
			}
			ts.read(tbuf, twidth);
			vs.read(vbuf, vwidth);
			int newts = *((int*) tbuf);
			int newvs = *((int*) vbuf);
			if (newts <= oldts) {
				++nolder;
			} else {
				//opentsdb wire format:
				//put http.hits 1234567890 34877
				//put <metric> <timestamp> <value>
				//batch import format has no "put"
				sprintf(insertbuf, "%s %d %d t=v",
						metricname,
						newts,
						newvs);
				cout << insertbuf << endl;

				++ninserts;
				oldts = newts;
			}
		}

		if (0 != nolder) {
			cerr << "Got timestamps older than stream tail!" << endl;
			cerr << "  count: " << nolder << endl;
			cerr << "  in " << *it << "" << endl;
		}

		timeit(false, ninserts);

		ts.close();
		vs.close();
	}
}

/**
 * @brief Write opentsdb stream to stdout for tagged metrics
 * the keys are simply increasing values for a metric name, prefixed by "m"
 */
void print_opentsdb_inserts_multi(DataMulti data, int twidth, int vwidth) {
	char tbuf[twidth];
	char vbuf[vwidth];

	char metricname[1024];
	char insertbuf[4096];
	int metricdx = 0;

	for (map<string, set<string> >::const_iterator it = data.begin(); it != data.end(); ++it) {
		++metricdx;
		sprintf(metricname, "m%d", metricdx);

		int tagid = 0;

		string tsname = data.get_name((*it).first, TS);

		for (set<string>::const_iterator sit = (*it).second.begin(); sit != (*it).second.end(); ++sit) {
			long ninserts = 0;
			long nolder = 0;

			++tagid;

			string vsname = data.get_name(*sit, VS);
			ifstream ts(tsname.c_str(), ios::binary | ios::in);
			ifstream vs(vsname.c_str(), ios::binary | ios::in);
			cerr << "inserting " << *sit << endl;

			if (!ts.good()) {
				cerr << "could not find timestamp file " << tsname << endl;
				break;
			}
			if (!vs.good()) {
				cerr << "could not find value file " << vsname << endl;
			}

			timeit(true);

			int oldts = 0;
			while (ts.good()) {
				if (!vs.good()) {
					cerr << "values were not the same length as timestamps!" << endl;
					break;
				}
				ts.read(tbuf, twidth);
				vs.read(vbuf, vwidth);
				int newts = *((int*) tbuf);
				int newvs = *((int*) vbuf);
				if (newts <= oldts) {
					++nolder;
				} else {
					//opentsdb wire format:
					//put http.hits 1234567890 34877
					//put <metric> <timestamp> <value>
					//batch import format has no "put"
					sprintf(insertbuf, "%s %d %d t=%d",
							metricname,
							newts,
							newvs,
							tagid);
					cout << insertbuf << endl;

					++ninserts;
					oldts = newts;
				}
			}

			if (0 != nolder) {
				cerr << "Got timestamps older than stream tail!" << endl;
				cerr << "  count: " << nolder << endl;
				cerr << "  in " << *sit << "" << endl;
			}

			timeit(false, ninserts);

		}
	}
}

void test_opentsdb() {
	const char* tsloc = "../testdata-single/ts";
	const char* vsloc = "../testdata-single/vs";
	Data data(tsloc, vsloc);
	//print_opentsdb_metrics(data, cout);
	print_opentsdb_inserts(data, 4, 4);
}

void test_opentsdb_multi() {
	DataMulti data("../testdata-multi/ts.merge", "../testdata-multi/vs",
			getMergeMap("../testdata-multi/ts.merge/fmerge.sorted.txt"));
	print_opentsdb_inserts_multi(data, 4, 4);
}

#endif /* OPENTSDB_HPP_ */
