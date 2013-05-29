/*
 * sqlite.hpp
 * SQLite insert interface
 *
 */

#ifndef SQLITE_HPP_
#define SQLITE_HPP_

#include "../inc/sqlite3.h"
#include "data.hpp"

using namespace std;

/**
 * @param db db pointer to run query against
 * @param sql query to execute
 * @returns 0 on no error, 1 otherwise
 */
int check_exec(sqlite3* db, const char *sql) {
	char *local_err;
	if (sqlite3_exec(db, sql, NULL, NULL, &local_err)) {
		cerr << local_err << endl;
		return 1;
	}
	return 0;
}

/**
 * Open a new sqlite db at the given file
 */
sqlite3* new_sqlite_db(const char* output) {
	sqlite3 *db;
	int rc = sqlite3_open(output, &db);
	if (rc) {
		cerr << "couldn't open db" << sqlite3_errmsg(db) << endl;
		sqlite3_close(db);
		return NULL;
	}

	check_exec(db, "PRAGMA synchronous=OFF");
	check_exec(db, "PRAGMA count_changes=OFF");
	check_exec(db, "PRAGMA journal_mode=MEMORY");
	check_exec(db, "PRAGMA temp_store=MEMORY");

	return db;
}

/**
 * @brief Insert timestamp column per value column (not used for evaluation)
 * @param data source
 * @param twidth timestamp width
 * @param vwidth value width
 * @param output sqlite file location to write
 */
void insert_sqlite(Data data, int twidth, int vwidth, const char* output) {
	sqlite3* db = new_sqlite_db(output);

	char tbuf[twidth];
	char vbuf[vwidth];

	//entries:
	//copy(diritems.begin(), diritems.end(), ostream_iterator<string>(cout, " "));

	for (set<string>::const_iterator it = data.begin(); it != data.end(); ++it) {
		long ninserts = 0;
		long nfailures = 0;

		timeit(true);

		check_exec(db, "BEGIN TRANSACTION");

		//assume existence of both ts and vs. amd that they're the same length
		ifstream ts(data.get_name(*it, TS).c_str(), ios::binary | ios::in);
		ifstream vs(data.get_name(*it, VS).c_str(), ios::binary | ios::in);
		cerr << "inserting " << *it << endl;

		string createsql = string("create table ") +
				*it +
				string("(time integer primary key on conflict ignore, val integer)");
		check_exec(db, createsql.c_str());

		string isql = string("insert into ") +
				*it +
				string(" VALUES(?1, ?2)");
		const char *buf = isql.c_str();
		sqlite3_stmt* stmt;
		sqlite3_prepare_v2(db, buf, strlen(buf), &stmt, NULL);

		if (!ts.good()) {
			cerr << "could not find timestamp file." << endl;
			break;
		}

		while (ts.good()) {
			if (!vs.good()) {
				cerr << "values were not the same length as timestamps!" << endl;
				break;
			}
			ts.read(tbuf, twidth);
			vs.read(vbuf, vwidth);

			sqlite3_bind_int(stmt, 1, *((int*) tbuf));
			sqlite3_bind_int(stmt, 2, *((int*) vbuf));
			int rc = sqlite3_step(stmt);
			if (rc != SQLITE_DONE) {
				++nfailures;
				cerr << "insert failed. Error was: ";
				cerr << sqlite3_errmsg(db) << endl;
				cerr << "Moving to next table." << endl;
				break;
			} else {
				++ninserts;
			}
			sqlite3_reset(stmt);
		}

		check_exec(db, "COMMIT TRANSACTION");

		timeit(false, ninserts);

		ts.close();
		vs.close();
	}
}

/**
 * @brief Insert schemaed data
 * @param data source
 * @param twidth timestamp width
 * @param vwidth value width
 * @param output sqlite file location to write
 */
void insert_sqlite_multi(DataMulti data, int twidth, int vwidth, const char* output) {
	sqlite3* db = new_sqlite_db(output);

	for (map<string, set<string> >::const_iterator it = data.begin(); it != data.end(); ++it) {
		long ninserts = 0;

		check_exec(db, "BEGIN TRANSACTION");

		timeit(true);

		//grab the timestamp stream
		string tsloc = data.get_name((*it).first, TS);
		ifstream ts(tsloc.c_str(), ios::binary | ios::in);
		vector<ifstream*> vs((*it).second.size());

		//build the create table and insert statements
		//  for the time stream and value streams
		stringstream createsql;
		createsql << "create table t" << (*it).first;
		createsql << "(time integer primary key on conflict ignore";

		cerr << "tsloc was" << tsloc << endl;

		stringstream isql;
		isql << "insert into t" << (*it).first << " VALUES(?1";

		int dx = 0;
		for (set<string>::const_iterator sit = (*it).second.begin(); sit != (*it).second.end(); ++sit) {
			string vloc = data.get_name(*sit, VS);
			vs[dx] = new ifstream(vloc.c_str(), ios::binary | ios::in);
			if (!vs[dx]->good()) {
				cerr << "Missing value stream: " << vloc << endl;
			}
			createsql << ", " << *sit << " integer";

			isql << ", ?";
			isql << (dx+2);

			++dx;
		}

		createsql << ")";
		isql << ")";

		//create the table!
		check_exec(db, createsql.str().c_str());
		cerr << "created table: " << createsql.str() << endl;

		char tbuf[twidth];
		char vbufs[vs.size()][vwidth];

		//prep the insert statement.
		string isqlstr = isql.str();
		char* cstr = new char[isqlstr.length()+1];
		strcpy(cstr, isqlstr.c_str());
		sqlite3_stmt* stmt;
		sqlite3_prepare_v2(db, cstr, -1, &stmt, NULL);

		cerr << "inserting: " << cstr << endl;

		if (!ts.good()) {
			cerr << "could not find timestamp file " << tsloc << endl;
			break;
		}

		//how many value streams couldn't we insert?
		unsigned n_vstream_failures = 0;

		while (ts.good()) {
			ts.read(tbuf, twidth);
			sqlite3_bind_int(stmt, 1, *((int*) tbuf));

			//grab a value from each value stream and insert
			//	(hope that the ifstreams will be buffered)
			for (unsigned i = 0; i < vs.size(); ++i) {
				if (!vs[i]->good()) {
					++n_vstream_failures;
					vs[i]->read(vbufs[i], vwidth);
					sqlite3_bind_int(stmt, i+2, *((int*) vbufs[i]));
				} else {
					//XXX: do something better about missing value stream entries?
					// for now insert zero
					sqlite3_bind_int(stmt, i+2, 0);
				}
			}

			int rc = sqlite3_step(stmt);
			if (rc != SQLITE_DONE) {
				cerr << "insert failed. Error was: ";
				cerr << sqlite3_errmsg(db) << endl;
				cerr << "Moving to next table." << endl;
				break;
			} else {
				++ninserts;
			}

			sqlite3_reset(stmt);
		}

		if (0 != n_vstream_failures) {
			cerr << "some value streams were incomplete or missing: count="
					<< n_vstream_failures << endl;
		}

		check_exec(db, "COMMIT TRANSACTION");

		timeit(false, ninserts);


		for (unsigned i = 0; i < vs.size(); ++i) {
			vs[i]->close();
			delete vs[i];
		}
		ts.close();
	}
}

void test_insert_sqlite_multi() {
	DataMulti data("../testdata-multi/ts.merge", "../testdata-multi/vs",
			getMergeMap("../testdata-multi/ts.merge/fmerge.sorted.txt"));
	insert_sqlite_multi(data, 4, 4, "out.db");
}

#endif /* SQLITE_HPP_ */
