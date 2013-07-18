/**
 * Insert binary {ts,vs} into databases 
 */

#include <iostream>
#include <fstream>
#include <algorithm>
#include <iterator>
#include <set>
#include <map>
#include <vector>
#include <string>
#include <sstream>

#include <cstring>
#include <ctime>
#include <cstdlib>

#include "data.hpp"
#include "opentsdb.hpp"
#include "sqlite.hpp"
#include "csv.hpp"

#ifdef HAS_FINANCEDB
#include "financedb.hpp"
#else
void insert_financedb_multi(DataMulti dm, int tw, int vw) {
	cerr << "Unimplemented" << endl;
}
#endif

using namespace std;

void usage(char** argv) {
	cout << "Usage: " << argv[0] << " [fn] [args]" << endl;
	cout << "  if [fn] is test, run basic tests." << endl;
	cout << "  if [fn] is ins_{sqlite,financedb,opentsdb,csv}_multi, [args] = " << endl;
	cout << "    2: timestamp directory" << endl;
	cout << "    3: values directory" << endl;
	cout << "    4: timestamp merge map" << endl;
	cout << "    5: output database" << endl;
	cout << "  if [fn] is ins_opentsdb, [args] = " << endl;
	cout << "    2: timestamp directory" << endl;
	cout << "    3: values directory" << endl;
	cout << "    4: metric name file" << endl;
	cout << "    (writes inserts to stdout)" << endl;
}

int main(int argc, char** argv) {
	if (argc < 2) {
		usage(argv);
		return 1;
	}

	string fn(argv[1]);

	int twidth = 4;
	int vwidth = 4;

	if (fn == "test") {
		/*
		test_datamulti();
		test_mergemap();
		test_opentsdb_multi();
		test_insert_sqlite_multi();
		*/
		test_insert_csv_multi();
	} else if (fn == "ins_sqlite_multi") {
		DataMulti data(argv[2], argv[3], getMergeMap(argv[4]));
		insert_sqlite_multi(data, twidth, vwidth, argv[5]);
	} else if (fn == "ins_opentsdb") {
		ofstream metout(argv[4], ios::out);
		Data data(argv[2], argv[3]);
		print_opentsdb_metrics(data, metout);
		metout.close();

		print_opentsdb_inserts(data, twidth, vwidth);
	} else if (fn == "ins_opentsdb_multi") {
		DataMulti dm(argv[2], argv[3], getMergeMap(argv[4]));
		Data wrapper(dm);
		print_opentsdb_inserts(wrapper, twidth, vwidth);
	} else if (fn == "ins_financedb_multi") {
		DataMulti data(argv[2], argv[3], getMergeMap(argv[4]));
		insert_financedb_multi(data, twidth, vwidth);
	} else if (fn == "ins_csv_multi") {
		DataMulti data(argv[2], argv[3], getMergeMap(argv[4]));
		insert_csv_multi(data, twidth, vwidth, argv[5]);
	}

	return 0;
}
