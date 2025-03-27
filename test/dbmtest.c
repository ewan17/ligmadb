/**
 * @note    gcc -DTEST -I../include -g -o dbmtest dbmtest.c ../src/utilities.c -llmdb
 */
#include "../src/db.c"
#include "pthread.h"

#define NUM_THREADS 14
#define LMDB_DEFAULT_READERS 126
#define LOCAL_READER_COUNT (LMDB_DEFAULT_READERS / NUM_THREADS)

const char *filename = "dbmtest/";

void *read_thread_function(void *arg) {
    int thread_id = *(int *)arg;
    printf("Thread -- %d!\n", thread_id);

    sleep(1);

    init_thread_local_readers(LOCAL_READER_COUNT);

    TrashTxn *tt1,*tt2,*tt3;
    MDB_val key,val;

    assert(trash_txn(&tt1, "db1", TRASH_RD_TXN) == TRASH_SUCCESS);
    assert(trash_txn(&tt2, "db2", TRASH_RD_TXN) == TRASH_SUCCESS);
    assert(trash_txn(&tt3, "db3", TRASH_RD_TXN) == TRASH_SUCCESS);

    char res[8] = {0};

    key.mv_data = "testkey1";
    key.mv_size = 8;
    assert(trash_get(tt1, &key, &val) == TRASH_SUCCESS);
    memcpy(&res, (char *)val.mv_data, val.mv_size);
    assert(strcmp("testval1", res) == 0);

    key.mv_data = "testkey2";
    key.mv_size = 8;
    assert(trash_get(tt2, &key, &val) == TRASH_SUCCESS);
    memcpy(&res, (char *)val.mv_data, val.mv_size);
    assert(strcmp("testval2", res) == 0);

    key.mv_data = "testkey3";
    key.mv_size = 8;
    assert(trash_get(tt3, &key, &val) == TRASH_SUCCESS);
    memcpy(&res, (char *)val.mv_data, val.mv_size);
    assert(strcmp("testval3", res) == 0);

    return_txn(tt1);
    return_txn(tt2);
    return_txn(tt3);

    clean_thread_local_readers();

    free(arg);
    pthread_exit(NULL);
}

void *write_thread_function(void *arg) {
    int thread_id = *(int *)arg;
    printf("Thread -- %d!\n", thread_id);

    init_thread_local_readers(LOCAL_READER_COUNT);

    struct DbMeta dbmeta;

    dbmeta.flags = MDB_CREATE;
    dbmeta.name = "db1";
    dbmeta.slots = 1;
    assert(write_db_meta(&dbmeta) == TRASH_SUCCESS);

    dbmeta.flags = MDB_CREATE;
    dbmeta.name = "db2";
    dbmeta.slots = 1;
    assert(write_db_meta(&dbmeta) == TRASH_SUCCESS);

    dbmeta.flags = MDB_CREATE;
    dbmeta.name = "db3";
    dbmeta.slots = 1;
    assert(write_db_meta(&dbmeta) == TRASH_SUCCESS);

    TrashTxn *tt;
    MDB_val key,val;

    key.mv_data = "testkey1";
    key.mv_size = 8;
    val.mv_data = "testval1";
    val.mv_size = 8;
    assert(trash_txn(&tt, "db1", TRASH_WR_TXN) == TRASH_SUCCESS);
    assert(trash_put(tt, &key, &val, 0) == TRASH_SUCCESS);

    change_txn_db(tt, "db2");

    key.mv_data = "testkey2";
    key.mv_size = 8;
    val.mv_data = "testval2";
    val.mv_size = 8;
    assert(trash_put(tt, &key, &val, 0) == TRASH_SUCCESS);

    change_txn_db(tt, "db3");

    key.mv_data = "testkey3";
    key.mv_size = 8;
    val.mv_data = "testval3";
    val.mv_size = 8;
    assert(trash_put(tt, &key, &val, 0) == TRASH_SUCCESS);

    return_txn(tt);

    clean_thread_local_readers();

    free(arg);
    pthread_exit(NULL);
}

int main(int argc, char *argv[]) {
    pthread_t threads[NUM_THREADS];
    int rc;

    assert(open_env(DB_SIZE, NUM_DBS, LMDB_DEFAULT_READERS) == 0);

    for (int i = 0; i < NUM_THREADS; i++) {
        void *(*func)(void *);
        func = read_thread_function;

        int *thread_id = malloc(sizeof(int));
        if(thread_id == NULL) {
            fprintf(stderr, "Memory allocation failed\n");
            exit(1);
        }
        *thread_id = i;

        if(i == 0)
            func = write_thread_function;

        rc = pthread_create(&threads[i], NULL, func, (void *)thread_id);
        if(rc) {
            fprintf(stderr, "Error creating thread %d, return code: %d\n", i, rc);
            exit(1);
        }
    }

    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }

    close_db("db1");
    close_db("db2");
    close_db("db3");
    close_db(METADATA);
    close_env();

    return 0;
}