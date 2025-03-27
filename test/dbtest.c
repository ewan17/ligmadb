/**
 * @note    gcc -DTEST -I../include -g -o dbtest dbtest.c ../src/utilities.c -llmdb
 */
#include "../src/db.c"

const char *filename = "dbtest/";

void db_test1() {
    TrashTxn *tt;
    struct DbMeta *dbmeta;
    int rc;

    const char *dbname = "test1";
    const char *notdbname = "not_test";

    dbmeta = (struct DbMeta *)trash_malloc(sizeof(struct DbMeta));
    dbmeta->flags = MDB_CREATE;
    dbmeta->name = dbname;
    dbmeta->slots = 1;

    assert(trash_txn(&tt, METADATA, TRASH_RD_TXN) == TRASH_SUCCESS);
    assert(strcmp(METADATA, tt->dbs[tt->dbscount - 1]->name) == 0);

    assert(change_txn_db(tt, notdbname) == TRASH_DB_DNE);

    assert(write_db_meta(dbmeta) == TRASH_SUCCESS);
    assert(change_txn_db(tt, dbname) == TRASH_SUCCESS);
    assert(strcmp(dbname, tt->dbs[tt->dbscount - 1]->name) == 0);

    return_txn(tt);

    struct OpenDb *db;
    db = internal_get_open_db(dbname);
    assert(db->name == dbname);

    close_db(dbname);
    assert(internal_get_open_db(dbname) == NULL);
    free(dbmeta);
}

void db_test2() {
    TrashTxn *tt;
    MDB_val key,val,res;
    struct DbMeta *dbmeta;
    char result[256];
    int rc;

    const char *dbname = "test2";

    dbmeta = (struct DbMeta *)trash_malloc(sizeof(struct DbMeta));
    dbmeta->flags = MDB_CREATE;
    dbmeta->name = dbname;
    dbmeta->slots = 1;

    assert(trash_txn(&tt, dbname, TRASH_WR_TXN) == TRASH_DB_DNE);
    assert(write_db_meta(dbmeta) == TRASH_SUCCESS);
    free(dbmeta);

    key.mv_data = "testkey";
    key.mv_size = 7;
    val.mv_data = "testval";
    val.mv_size = 7;
    assert(trash_txn(&tt, dbname, TRASH_WR_TXN) == TRASH_SUCCESS);
    assert(trash_put(tt, &key, &val, 0) == TRASH_SUCCESS);

    return_txn(tt);

    MDB_txn *ttc;
    struct OpenDb *db;
    db = internal_get_open_db(dbname);
    assert(db != NULL);

    mdb_txn_begin(oEnv->env, NULL, MDB_RDONLY, &ttc);

    key.mv_data = "testkey";
    key.mv_size = 7;
    assert(mdb_get(ttc, db->dbi, &key, &res) == 0);

    memcpy(result, res.mv_data, res.mv_size);
    result[res.mv_size] = '\0';
    assert(strcmp("testval", result) == 0);

    mdb_txn_abort(ttc);

    close_db(dbname);
    assert(internal_get_open_db(dbname) == NULL);
}

void db_test3() {
    TrashTxn *tt;
    TrashCursor *tc;
    MDB_val key, val;
    int rc;

    assert(trash_txn(&tt, METADATA, TRASH_WR_TXN) == TRASH_SUCCESS);
    assert(trash_cursor(&tc, tt) == TRASH_DB_SUCCESS);
    for(unsigned int i = 0; i < 10; i++) {
        char keybuf[5];
        char valbuf[5];

        snprintf(keybuf, sizeof(keybuf), "%s%d", "key", i);
        snprintf(valbuf, sizeof(valbuf), "%s%d", "val", i);

        key.mv_data = keybuf;
        key.mv_size = sizeof(keybuf);
        val.mv_data = valbuf;
        val.mv_size = sizeof(valbuf);
        assert((rc = trash_cur_put(tc, &key, &val, MDB_NOOVERWRITE)) == 0);
    }

    return_cursor(tc);
    return_txn(tt);

    assert(trash_txn(&tt, METADATA, TRASH_RD_TXN) == TRASH_SUCCESS);
    assert(trash_cursor(&tc, tt) == TRASH_DB_SUCCESS);

    key.mv_size = 3;
    key.mv_data = "key";

    MDB_cursor_op op = MDB_SET_RANGE;
    for(unsigned int i = 0; i < 10; i++) {
        char exp[5], res[5];

        assert(trash_cur_get(tc, &key, &val, op) == 0);
        snprintf(exp, sizeof(exp), "%s%d", "val", i);

        memcpy(&res, (char *)val.mv_data, val.mv_size);

        assert(strcmp(exp, res) == 0);

        op = MDB_NEXT;
    }

    return_cursor(tc);
    return_txn(tt);
}

int main(int argc, char *argv[]) {
    assert(open_env(DB_SIZE, NUM_DBS, 3) == 0);

    init_thread_local_readers(2);

    // db_test1();
    // db_test2();
    db_test3();
    
    clean_thread_local_readers();

    close_db(METADATA);
    close_env();

    return 0;
}