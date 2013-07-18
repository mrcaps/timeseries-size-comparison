/*
 * csv.hpp
 *
 *  Created on: Jul 17, 2013
 *      Author: ishafer
 */

#ifndef CSV_HPP_
#define CSV_HPP_

#include "data.hpp"

using namespace std;

void insert_csv_multi(DataMulti data, int twidth, int vwidth, const char *output) {
	//index of the current file ("table")
	int filedx = 0;

	//if we want to write multiple dsdefs to a single file
	//stringstream dsdef;

	for (map<string, set<string> >::const_iterator it = data.begin(); it != data.end(); ++it) {
		long ninserts = 0;

		++filedx;

		timeit(true);

		stringstream dsname;
		dsname << output << "-" << filedx << ".xml";
		ofstream dsdef(dsname.str().c_str(), ios::binary | ios::out);

		//grab the timestamp stream
		string tsloc = data.get_name((*it).first, TS);
		ifstream ts(tsloc.c_str(), ios::binary | ios::in);
		vector<ifstream*> vs((*it).second.size());

		//write DataSeries xml spec
		dsdef << "<ExtentType name=\"" << filedx << "\" namespace=\"tss.mrcaps.com\" version=\"1.0\">" << endl;
		//write timestamp line
		dsdef << "\t<field type=\"int32\" name=\"ts\" pack_relative=\"ts\" />" << endl;

		int coldx = 0;
		for (set<string>::const_iterator sit = (*it).second.begin(); sit != (*it).second.end(); ++sit) {
			string vloc = data.get_name(*sit, VS);
			vs[coldx] = new ifstream(vloc.c_str(), ios::binary | ios::in);

			++coldx;
			dsdef << "\t<field type=\"int32\" name=\"" << coldx << "\" />" << endl;
		}
		dsdef << "</ExtentType>" << endl;

		char tbuf[twidth];
		char vbufs[vs.size()][vwidth];

		if (!ts.good()) {
			cerr << "could not find timestamp file " << tsloc << endl;
			break;
		}

		int n_vstream_failures = 0;

		ofstream ssdata;

		stringstream name;
		name << output << "-" << filedx << ".csv";
		ssdata.open(name.str().c_str(), ios::binary | ios::out);
		while (ts.good()) {
			ts.read(tbuf, twidth);

			ssdata << *((int*) tbuf) << ",";

			//grab a value from each value stream and insert
			//	(hope that the ifstreams will be buffered)
			for (unsigned i = 0; i < vs.size(); ++i) {
				int v = 0;
				if (vs[i]->good()) {
					vs[i]->read(vbufs[i], vwidth);
					v = *((int*) vbufs[i]);
				} else {
					++n_vstream_failures;
				}

				ssdata << v;

				if (i != vs.size() - 1) {
					ssdata << ",";
				}
			}

			++ninserts;

			ssdata << endl;
		}

		if (0 != n_vstream_failures) {
			cerr << "some value streams were incomplete or missing: count="
					<< n_vstream_failures << endl;
		}

		timeit(false, ninserts);

		ssdata.close();

		for (unsigned i = 0; i < vs.size(); ++i) {
			vs[i]->close();
			delete vs[i];
		}
		ts.close();
	}
}

void test_insert_csv_multi() {
	DataMulti data("../testdata-multi/ts.merge", "../testdata-multi/vs",
			getMergeMap("../testdata-multi/ts.merge/fmerge.sorted.txt"));
	insert_csv_multi(data, 4, 4, "out-csv");
}

#endif /* CSV_HPP_ */
