#ifndef DB_H
#define DB_H

#include "lmdb.h"

#define DEFAULT_VAR_PATH "/var/local/trashdb/"

#ifdef TEST
#define DB_DIR "./var/local/trashdb/"
#else
#define DB_DIR DEFAULT_VAR_PATH "data/"
#endif
#define DB_DIR_LEN sizeof(DB_DIR) - 1

#define METADATA "metadata"

#define TRASH_DB_SUCCESS 0
#define TRASH_DB_ERROR -1

#define TRASH_DB_EXISTS 0x04
#define TRASH_DB_DNE 0x08

#define TRASH_TXN_INVALID 666
#define TRASH_CUR_INVALID 777
#define TRASH_OUT_OF_READER_SLOTS 888

#define TRASH_RD_TXN 0x01
#define TRASH_WR_TXN 0x02
#define TRASH_TXN_COMMIT 0X04
#define TRASH_TXN_RENEW 0x08

#define TRASH_DB_NAME_LEN 256
#define TRASH_DB_OPENED 0
#define TRASH_DB_WRITE_META 1

#define TRASH_DB_SIZE 10485760
#define TRASH_MAX_READERS 126
#define TRASH_NUM_DBS 50

/**
 * @note    currently only support 1 lmdb file
 */ 
extern const char *filename;

typedef struct TrashTxn TrashTxn;
typedef struct TrashCursor TrashCursor;

struct DbMeta {
    const char *name;
    unsigned int flags;
    unsigned int slots;
};

void init_thread_local_readers(size_t numrdrs);
void clean_thread_local_readers();

int open_env(size_t dbsize, unsigned int numdbs, unsigned int numthreads);
void close_env();
void emergency_cleanup();

int change_txn_db(TrashTxn *tt, const char *dbname);
// int nest_txn(TrashTxn **tt, const char *db, TrashTxn *pTxn);

// int open_db(TrashTxn *tt, const char *dbname);
void close_db(const char *dbname);
int write_db_meta(struct DbMeta *db);

int trash_txn(TrashTxn **tt, const char *dbname, int rd);
void return_txn(TrashTxn *tt);
int trash_cursor(TrashCursor **cur, TrashTxn *tt);
void return_cursor(TrashCursor *cur);
int trash_put(TrashTxn *tt, MDB_val *key, MDB_val *val, unsigned int flags);
int trash_get(TrashTxn *tt, MDB_val *key, MDB_val *data);
int trash_cur_put(TrashCursor *tc, MDB_val *key, MDB_val *val, unsigned int flags);
int trash_cur_get(TrashCursor *tc, MDB_val *key, MDB_val *val, MDB_cursor_op op);

#endif //DB_H