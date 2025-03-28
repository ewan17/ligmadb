#include <errno.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <assert.h>

#include "db.h"

#include "c-utils/utilities.h"
#include "c-utils/il.h"

#define METADATA_FLAGS MDB_CREATE

#define META_META "meta_meta"
#define META_META_LEN strlen(META_META)

#define DB_PREFIX "dbs:"
#define DB_PREFIX_LEN 4
#define DB_METADATA_KEY_FORMAT DB_PREFIX "%s"

#define INVALID_DB_ID -1

enum EnvState {
    ENV_OPEN,
    ENV_CLOSE
};

enum DbState {
    DB_OPEN,
    DB_CLOSE
};

enum RWTxn {
    READ,
    WRITE
};

struct TrashCursor {
    MDB_cursor *cur;
    enum RWTxn rw;
    struct OpenDb *db;
};

struct OpenDb {
    struct IL moveenv;
    
    const char *name;
    MDB_dbi dbi;
    enum DbState state;

    TrashCursor **curs;
    unsigned int curcount;
    unsigned int txncount;

    pthread_mutex_t odbMutex;
    pthread_cond_t odbCond;
};

struct TrashTxn {
    MDB_txn *txn;

    unsigned int actions;

    void (*fin_db)(struct OpenDb *);
    struct OpenDb *(*open_db)(const char *dbname);

    struct TrashCursor *cur;

    struct OpenDb **dbs;
    size_t dbscount;
    size_t dbscap;
};

struct OpenEnv {
    MDB_env *env;
    unsigned int envFlags;
    enum EnvState state;
    struct LL dbs;
    pthread_rwlock_t envLock;
};

struct Readers {
    TrashTxn **txns;
    size_t numTxns;
    size_t cap;
};

/** INTERNAL FUNCTIONS **/
static void init_metadata();
static void internal_create_open_env(struct OpenEnv **oEnv, MDB_env *env);
static int internal_set_env_fields(MDB_env *env, size_t dbsize, unsigned int numdbs, unsigned int numthreads);
static void internal_open_all_db(TrashTxn *tt);
static struct OpenDb *internal_add_db(struct DbMeta *dbmeta, MDB_dbi id);
static int internal_add_db_curs(struct OpenDb *db, TrashTxn *tt);
static void internal_add_db_txn(TrashTxn *tt, struct OpenDb *db);
static int db_match(struct OpenDb *db, const char *dbname);
static void internal_txn_handler(TrashTxn *tt);
static void internal_fin_db(struct OpenDb *db);
static void internal_close_db(struct OpenDb *db);
static int internal_begin_txn(TrashTxn **tt, int rd);
static TrashTxn *internal_get_read_txn();
static void internal_create_trash_txn(TrashTxn **tt, MDB_txn *txn, int rdwr);
static void internal_create_trash_cursor(TrashCursor **tc, struct OpenDb *db, enum RWTxn rw);
static void internal_fin_db_locked(struct OpenDb *db);
static void internal_db_txn_decrement(struct OpenDb *db, void (*fin_db)(struct OpenDb *));
static struct OpenDb *internal_get_open_db(const char *dbname);
static struct OpenDb *internal_get_open_db_locked(const char *dbname);

// environment that is open
static struct OpenEnv *oEnv = NULL;
// tls for the reader txns
static __thread struct Readers *rdrPool = NULL;

void init_thread_local_readers(size_t numrdrs) {
    int rc;

    if(rdrPool != NULL)
        return;

    rdrPool = (struct Readers *)malloc(sizeof(struct Readers));

    rdrPool->txns = calloc(numrdrs, sizeof(TrashTxn *));
    rdrPool->numTxns = numrdrs;
    rdrPool->cap = numrdrs;

    for (size_t i = 0; i < numrdrs; i++) {
        TrashTxn *tt;
        MDB_txn *txn;
        
        /**
         * @todo    build an error handling function for lmdb error messages
        */
        rc = mdb_txn_begin(oEnv->env, NULL, MDB_RDONLY, &txn);
        assert(rc == 0);

        internal_create_trash_txn(&tt, txn, TRASH_RD_TXN);
        mdb_txn_reset(txn);

        rdrPool->txns[i] = tt;
    }
}

void clean_thread_local_readers() {
    if(rdrPool == NULL)
        return;

    for (size_t i = 0; i < rdrPool->numTxns; i++) {
        TrashTxn *tt = rdrPool->txns[i];
        mdb_txn_abort(tt->txn);
        free(tt->dbs);
        free(tt);
        rdrPool->txns[i] = NULL;
    }

    free(rdrPool->txns);
    free(rdrPool);
}

/**
 * @todo    this function needs logging for errors
 * @todo    check the length of the file name or pass the length of the filename as a parameter in the function
*/
int open_env(size_t dbsize, unsigned int numdbs, unsigned int numrdrs) {
    MDB_env *env;
    struct stat st;
    char filepath[256];
    int rc;

    if(oEnv != NULL) {
        return TRASH_DB_SUCCESS;
    }

    if(validate_filename(filename) != 0) {
        return TRASH_DB_ERROR;
    }   

    snprintf(filepath, sizeof(filepath), "%s%s", DB_DIR, filename);
    if(stat(filepath, &st) < 0) {
        if(errno = ENOENT) {
            size_t len = strlen(filepath);
            if((rc = trash_mkdir(filepath, len, 0755)) != 0) {
                return TRASH_DB_ERROR;
            }
        } else {
            return TRASH_DB_ERROR;
        }
    }

    mdb_env_create(&env);
    if((rc = internal_set_env_fields(env, dbsize, numdbs, numrdrs)) != TRASH_DB_SUCCESS) {
        return TRASH_DB_ERROR;
    }
    
    if((rc = mdb_env_open(env, filepath, MDB_NOTLS | MDB_NOSYNC | MDB_NOMETASYNC, 0664)) != 0) {
        /**
         * @todo    handle the different possible errors for an invalid open
        */
        fprintf(stderr, "Error creating LMDB environment: %s\n", mdb_strerror(rc));
        mdb_env_close(env);
        assert(0);
    };

    // setup open env struct
    internal_create_open_env(&oEnv, env);
    // open/create metadata db
    init_metadata(env);

    return TRASH_DB_SUCCESS;
}

void close_env() {
    mdb_env_close(oEnv->env);
    pthread_rwlock_destroy(&oEnv->envLock);
    free(oEnv);
    oEnv = NULL;
}

void emergency_cleanup() {
    struct IL *curr;
    struct OpenDb *db;

    pthread_rwlock_wrlock(&oEnv->envLock);
    oEnv->state = ENV_CLOSE;
    size_t len = oEnv->dbs.len;
    curr = (&oEnv->dbs.head)->next;
    for (size_t i = 0; i < len; i++) {
        db = CONTAINER_OF(curr, struct OpenDb, moveenv);
        curr = (curr)->next;
        internal_close_db(db);
    }

    if(oEnv != NULL)
        pthread_rwlock_unlock(&oEnv->envLock);
}

int trash_txn(TrashTxn **tt, const char *dbname, int rd) {
    struct OpenDb *db;
    int rc;

    db = internal_get_open_db_locked(dbname);
    if(db == NULL)
        return TRASH_DB_DNE;

    rc = internal_begin_txn(tt, rd);
    if(rc != TRASH_DB_SUCCESS)
        return rc;

    internal_add_db_txn(*tt, db);

    return rc;
}

int change_txn_db(TrashTxn *tt, const char *dbname) {
    struct OpenDb *db;
    int rc;

    if(tt == NULL)
        return TRASH_TXN_INVALID;

    if(tt->dbscount > 0) {
        db = tt->dbs[tt->dbscount - 1];
        rc = db_match(db, dbname);
        if(rc != TRASH_DB_ERROR) {
            return rc;
        }
    }

    db = tt->open_db(dbname);

    if(db == NULL) {
        /**
         * @todo    check the metadata table to see if we have a db with the following name
         * @todo    if so, open it
         */
        return TRASH_DB_DNE;
    }

    internal_add_db_txn(tt, db);
    
    return TRASH_DB_SUCCESS;
}

void return_txn(TrashTxn *tt) {
    if(tt == NULL)
        return;

    internal_txn_handler(tt);

    for (size_t i = 0; i < tt->dbscount; i++) {
        internal_db_txn_decrement(tt->dbs[i], tt->fin_db);
    }

    tt->dbscount = 0;

    return_cursor(tt->cur);

    // we do not save write txns
    if(tt->actions & TRASH_WR_TXN || rdrPool == NULL) {
        free(tt->dbs);
        free(tt);
    }
}

int trash_cursor(TrashCursor **tc, TrashTxn *tt) {
    struct OpenDb *db;

    if(tc == NULL)
        return TRASH_CUR_INVALID;

    if(tt == NULL) 
        return TRASH_TXN_INVALID;

    if(tt->dbscount == 0)
        return TRASH_DB_ERROR;

    db = tt->dbs[tt->dbscount - 1];

    if(tt->actions & TRASH_WR_TXN) {
        internal_create_trash_cursor(tc, db, WRITE);
        assert(mdb_cursor_open(tt->txn, db->dbi, &(*tc)->cur) == 0);
        return TRASH_DB_SUCCESS;
    } 

    pthread_mutex_lock(&db->odbMutex);
    while(db->curcount <= 0)
        pthread_cond_wait(&db->odbCond, &db->odbMutex);

    *tc = db->curs[--db->curcount];
    (*tc)->db = db; 
    assert(mdb_cursor_renew(tt->txn, (*tc)->cur) == 0);
    pthread_mutex_unlock(&db->odbMutex);
    
    return TRASH_DB_SUCCESS;
}

void return_cursor(TrashCursor *tc) {
    struct OpenDb *db;

    if(tc == NULL)
        return;

    if(tc->rw == WRITE) {
        mdb_cursor_close(tc->cur);
        free(tc);
        return;
    }

    db = tc->db;
    pthread_mutex_lock(&db->odbMutex);
    db->curs[db->curcount++] = tc;
    pthread_cond_signal(&db->odbCond);
    pthread_mutex_unlock(&db->odbMutex);
}

/**
 * If the db flags are in the metadata db, then those will be used.
 * If the db is not created yet, then the flags will have the MDB_CREATE and whatever other flags are passed in
 * 
 * @note    max db name is 255 chars
 * @todo    make sure only one thread is opening the requested database at a time
 * @todo    make sure the database is not being closed and opened at the same time as well
 */
// int open_db(TrashTxn *tt, const char *dbname) {
//     MDB_dbi id;
//     MDB_val key, data;
//     struct OpenDb *db;
//     struct DbMeta dbmeta;
//     char dbKeyName[TRASH_DB_NAME_LEN];
//     unsigned int flags;
//     int rc, res;

//     res = TRASH_DB_OPENED;

//     if(tt == NULL || (tt->actions & TRASH_WR_TXN))
//         return TRASH_TXN_INVALID;

//     rc = change_txn_db(tt, METADATA);
//     assert(rc == 0);

//     pthread_rwlock_wrlock(&oEnv->envLock);
//     db = internal_get_open_db(dbname);
//     if(db != NULL) {
//         pthread_rwlock_unlock(&oEnv->envLock);
//         tt->db = db;
//         return res;
//     }

//     key.mv_size = sizeof(dbKeyName);
//     key.mv_data = dbname;

//     rc = trash_get(tt, &key, &data);
//     if(rc == 0) {
//         // db was found
//         memcpy(&dbmeta, data.mv_data, data.mv_size);
//         flags = dbmeta.flags;
//     }else if(rc == MDB_NOTFOUND) {
//         return TRASH_DB_WRITE_META;
//     } else {
//         assert(0);
//     }

//     rc = mdb_dbi_open(tt->txn, dbname, flags, &id);
//     assert(rc == 0);

//     // add db to the env map
//     internal_add_db_curs(&tt->db, tt, &dbmeta, id);

//     pthread_rwlock_unlock(&oEnv->envLock);
//     return res;
// }

void close_db(const char *dbname) {
    struct OpenDb *db;

    pthread_rwlock_wrlock(&oEnv->envLock);
    db = internal_get_open_db(dbname);
    if(db == NULL) {
        pthread_rwlock_unlock(&oEnv->envLock);
        return;
    }
    internal_close_db(db);
    pthread_rwlock_unlock(&oEnv->envLock);
}

int write_db_meta(struct DbMeta *dbmeta) {
    TrashTxn *temp;
    MDB_dbi dbi;
    struct OpenDb *db;
    MDB_val key, val;
    int rc;

    pthread_rwlock_wrlock(&oEnv->envLock);
    db = internal_get_open_db(dbmeta->name);
    if(db != NULL) {
        pthread_rwlock_unlock(&oEnv->envLock);
        return TRASH_DB_SUCCESS;
    }

    internal_begin_txn(&temp, TRASH_WR_TXN);
    temp->fin_db = internal_fin_db;
    temp->open_db = internal_get_open_db;

    change_txn_db(temp, METADATA);
    
    rc = mdb_dbi_open(temp->txn, dbmeta->name, dbmeta->flags, &dbi);
    assert(rc == 0);

    char key_buf[TRASH_DB_NAME_LEN] = {0};
    snprintf(key_buf, TRASH_DB_NAME_LEN, DB_METADATA_KEY_FORMAT, dbmeta->name);
    key.mv_data = key_buf;
    key.mv_size = TRASH_DB_NAME_LEN;

    val.mv_size = sizeof(struct DbMeta);
    val.mv_data = dbmeta;

    rc = trash_put(temp, &key, &val, 0);
    assert(rc == 0);
    
    db = internal_add_db(dbmeta, dbi);

    temp->actions |= TRASH_TXN_COMMIT;
    return_txn(temp);
    
    internal_begin_txn(&temp, TRASH_RD_TXN);
    temp->fin_db = internal_fin_db;
    temp->open_db = internal_get_open_db;

    change_txn_db(temp, db->name);

    internal_add_db_curs(db, temp);
    return_txn(temp);

    pthread_rwlock_unlock(&oEnv->envLock);

    return TRASH_DB_SUCCESS;
}

/**
 * @todo    flags need to be checked with how the dbi was opened initially
 */
int trash_put(TrashTxn *tt, MDB_val *key, MDB_val *val, unsigned int flags) {
    struct OpenDb *db;
    int rc;
    
    if(tt->dbscount == 0 || tt->actions & TRASH_RD_TXN)
        return TRASH_DB_ERROR;

    db = tt->dbs[tt->dbscount - 1];
    rc = mdb_put(tt->txn, db->dbi, key, val, flags);
    if(rc == 0) {
        tt->actions |= TRASH_TXN_COMMIT;
    }
    return rc;
}

int trash_get(TrashTxn *tt, MDB_val *key, MDB_val *data) {
    struct OpenDb *db;
    int rc;
    
    if(tt->dbscount == 0)
        return TRASH_DB_ERROR;
    
    db = tt->dbs[tt->dbscount - 1];
    rc = mdb_get(tt->txn, db->dbi, key, data);
    return rc;
}

int trash_cur_put(TrashCursor *tc, MDB_val *key, MDB_val *val, unsigned int flags) {
    int rc;

    if(tc == NULL)
        return TRASH_DB_ERROR;

    rc = mdb_cursor_put(tc->cur, key, val, flags);
    return rc;
}

int trash_cur_get(TrashCursor *tc, MDB_val *key, MDB_val *val, MDB_cursor_op op) {
    int rc;

    if(tc == NULL)
        return TRASH_DB_ERROR;

    rc = mdb_cursor_get(tc->cur, key, val, op);
    return rc;
}


/**
 * @note    Should only be called on startup. no need for locks in this function only 1 thread should be running
 */
static void init_metadata() {
    TrashTxn *tt;
    MDB_txn *txn;
    MDB_dbi dbi;
    struct OpenDb *db;
    struct DbMeta dbmeta;
    int rc;

    rc = mdb_txn_begin(oEnv->env, NULL, 0, &txn);
    assert(rc == 0);

    dbmeta.name = METADATA;
    dbmeta.flags = METADATA_FLAGS;
    dbmeta.slots = 1;

    rc = mdb_dbi_open(txn, dbmeta.name, dbmeta.flags, &dbi);
    assert(rc == 0);
    mdb_txn_commit(txn);

    db = internal_add_db(&dbmeta, dbi);
    trash_txn(&tt, METADATA, TRASH_RD_TXN);
    internal_add_db_curs(db, tt);

    return_txn(tt);
}

static void internal_close_db(struct OpenDb *db) {
    pthread_mutex_lock(&db->odbMutex);
    if(db->txncount == 0) {
        internal_fin_db(db);
    } else {
        db->state = DB_CLOSE;
        pthread_mutex_unlock(&db->odbMutex);
    }
}

static void internal_open_all_db(TrashTxn *tt) {
    MDB_cursor *curr;
    MDB_dbi dbi;
    MDB_val key, val;
    struct OpenDb *db;
    struct DbMeta dbmeta;
    int rc;

    change_txn_db(tt, METADATA);
    // open all dbs if the key exists 
    // meaning the env has already been configured and we might have dbs created
    db = tt->dbs[tt->dbscount - 1];
    rc = mdb_cursor_open(tt->txn, db->dbi, &curr);
    assert(rc == 0);

    key.mv_size = DB_PREFIX_LEN;
    key.mv_data = DB_PREFIX;

    MDB_cursor_op op = MDB_SET_RANGE;
    while((rc = mdb_cursor_get(curr, &key, &val, op)) == 0) {
        struct OpenDb *db;
        char *dbKeyName;

        dbKeyName = (char *)key.mv_data;
        rc = memcmp(dbKeyName, DB_PREFIX, DB_PREFIX_LEN);
        if(rc != 0) {
            break;
        }

        memcpy(&dbmeta, val.mv_data, val.mv_size);

        rc = mdb_dbi_open(tt->txn, dbmeta.name, dbmeta.flags, &dbi);
        assert(rc == 0);
        db = internal_add_db(&dbmeta, dbi);
        internal_add_db_curs(db, tt);

        op = MDB_NEXT;
    }

    mdb_cursor_close(curr);
    // return_cursor(curr);
}


static void internal_txn_handler(TrashTxn *tt) {
    bool committed = false;

    if(tt->actions & TRASH_TXN_COMMIT) {
        assert(mdb_txn_commit(tt->txn) == 0);
        tt->actions &= ~TRASH_TXN_COMMIT;
        committed = true;
    }

    // abort write txns if not committed
    if((tt->actions & TRASH_WR_TXN) && !committed) {
        // abort if the txn was not commited
        mdb_txn_abort(tt->txn);
    }
    
    if(tt->actions & TRASH_RD_TXN) {
        // reset read txn if it has not been committed
        if(!committed) {
            mdb_txn_reset(tt->txn);
        }

        if(rdrPool == NULL || rdrPool->numTxns >= rdrPool->cap) {
            mdb_txn_abort(tt->txn);
            return;
        }

        rdrPool->txns[rdrPool->numTxns++] = tt;
    }
}

static void internal_fin_db_locked(struct OpenDb *db) {
    pthread_rwlock_wrlock(&oEnv->envLock);
    internal_fin_db(db);
    pthread_rwlock_unlock(&oEnv->envLock);
}

static void internal_fin_db(struct OpenDb *db) {
    mdb_dbi_close(oEnv->env, db->dbi);
    item_remove(&db->moveenv);
    /**
     *  @todo   locking issue with oEnv->dbs decrement  
    */ 
    oEnv->dbs.len--;

    pthread_mutex_destroy(&db->odbMutex);

    for (size_t i = 0; i < db->curcount; i++) {
        TrashCursor *tc = db->curs[i];
        mdb_cursor_close(tc->cur);
        free(tc);
    }

    free(db->curs);
    free(db);

    if(oEnv->state == ENV_CLOSE && oEnv->dbs.len == 0)
        close_env();
}

static void internal_db_txn_decrement(struct OpenDb *db, void (*fin_db)(struct OpenDb *)) {
    pthread_mutex_lock(&db->odbMutex);
    if(db->txncount > 0)
        db->txncount--;

    if(db->state == DB_CLOSE && db->txncount == 0) {
        fin_db(db);
    }
    pthread_mutex_unlock(&db->odbMutex);
}

static struct OpenDb *internal_get_open_db_locked(const char *dbname) {
    struct OpenDb *db;

    pthread_rwlock_rdlock(&oEnv->envLock);
    db = internal_get_open_db(dbname);
    pthread_rwlock_unlock(&oEnv->envLock);

    return db;
}

static struct OpenDb *internal_get_open_db(const char *dbname) {
    struct OpenDb *db = NULL;
    struct IL *curr;

    for_each(&oEnv->dbs.head, curr) {
        struct OpenDb *tempdb;
        tempdb = CONTAINER_OF(curr, struct OpenDb, moveenv);
        if(db_match(tempdb, dbname) == TRASH_DB_SUCCESS) {
            db = tempdb;
            break;
        }
    }

    return db;
}

static void internal_create_open_env(struct OpenEnv **oEnv, MDB_env *env) {
    *oEnv = (struct OpenEnv *)malloc(sizeof(struct OpenEnv));
    assert((*oEnv) != NULL);

    (*oEnv)->env = env;
    init_list(&(*oEnv)->dbs);

    pthread_rwlock_init(&(*oEnv)->envLock, NULL);

    (*oEnv)->envFlags = mdb_env_get_flags(env, &(*oEnv)->envFlags);
    (*oEnv)->state = ENV_OPEN;
}

/**
 * @note    this function will need to check if their is room for the desired db size
 * @todo    make this function better    
*/
static int internal_set_env_fields(MDB_env *env, size_t dbsize, unsigned int numdbs, unsigned int numrdrs) {
    if(mdb_env_set_maxdbs(env, numdbs) != 0) {
        return TRASH_DB_ERROR;
    }
    if(mdb_env_set_maxreaders(env, numrdrs) != 0) {
        return TRASH_DB_ERROR;
    }
    if(mdb_env_set_mapsize(env, dbsize) != 0) {
        return TRASH_DB_ERROR;
    }
    return TRASH_DB_SUCCESS;
}

static int db_match(struct OpenDb *db, const char *dbname) {
    if(db == NULL) {
        return TRASH_DB_ERROR;
    }

    int rc;
    rc = (strcmp(db->name, dbname) == 0) ? TRASH_DB_SUCCESS : TRASH_DB_ERROR;

    return rc;
}

/**
 * @note    the calling function needs to handle locking for the oenv struct
 */
static struct OpenDb *internal_add_db(struct DbMeta *dbmeta, MDB_dbi dbi) {
    struct OpenDb *db;

    db = (struct OpenDb *)malloc(sizeof(struct OpenDb));
    assert(db != NULL);

    db->curs = (TrashCursor **)calloc(dbmeta->slots, sizeof(TrashCursor *));
    db->curcount = dbmeta->slots;

    db->name = dbmeta->name;
    db->dbi = dbi;
    db->state = DB_OPEN;
    db->txncount = 0;
    pthread_mutex_init(&db->odbMutex, NULL);
    pthread_cond_init(&db->odbCond, NULL);

    init_il(&db->moveenv);
    list_append(&oEnv->dbs, &db->moveenv);

    return db;
}

static int internal_add_db_curs(struct OpenDb *db, TrashTxn *tt) {
    int rc;

    if(tt == NULL || (tt->actions & TRASH_WR_TXN)) {
        return TRASH_TXN_INVALID;
    }

    for (size_t i = 0; i < db->curcount; i++) {
        internal_create_trash_cursor(&db->curs[i], db, READ);
        rc = mdb_cursor_open(tt->txn, db->dbi, &(db->curs[i])->cur);
        assert(rc == 0);
    }

    return TRASH_DB_SUCCESS;
}

static void internal_add_db_txn(TrashTxn *tt, struct OpenDb *db) {
    pthread_mutex_lock(&db->odbMutex);
    db->txncount++;
    pthread_mutex_unlock(&db->odbMutex);

    if(tt->dbscount == tt->dbscap)
        assert(NULL);

    tt->dbs[tt->dbscount] = db;
    tt->dbscount++;
}

static int internal_begin_txn(TrashTxn **tt, int rdwr) {
    MDB_txn *txn;
    int rc, flags = 0;
    
    if(rdwr == TRASH_RD_TXN) {
        *tt = internal_get_read_txn();
        if(*tt == NULL)
            return TRASH_OUT_OF_READER_SLOTS;
    } else {
        rc = mdb_txn_begin(oEnv->env, NULL, flags, &txn);
        assert(rc == 0);

        internal_create_trash_txn(tt, txn, TRASH_WR_TXN);
    }

    return TRASH_DB_SUCCESS;
}

static TrashTxn *internal_get_read_txn() {
    TrashTxn *tt;

    if(rdrPool == NULL) {
        // the rdr pool should never be NULL unless the calling function is on env set up
        // in that case return a reader txn
        MDB_txn *txn;
        
        // create and rd txn and return
        mdb_txn_begin(oEnv->env, NULL, MDB_RDONLY, &txn);
        internal_create_trash_txn(&tt, txn, TRASH_RD_TXN);
        return tt;
    }

    if(rdrPool->numTxns == 0) {
        /**
         * @note    idk what to do here
         * @note    we run out of readers in the pool
         * @note    have to be careful creating new readers since we set a msx_readers arg for lmdb
         * 
         * @note    this is a problem when we begin nesting txns. how many can we nest per thread?
         */
        return NULL;
    }

    tt = rdrPool->txns[--rdrPool->numTxns];
    assert(mdb_txn_renew(tt->txn) == 0);

    return tt;
}

static void internal_create_trash_txn(TrashTxn **tt, MDB_txn *txn, int rdwr) {
    unsigned int actions = 0;

    *tt = (TrashTxn *)malloc(sizeof(TrashTxn));
    assert(*tt);

    /**
     * @todo    change the capacity later, should be the max num of dbs available
     */
    (*tt)->dbs = (struct OpenDb **)calloc(10, sizeof(struct OpenDb *));
    assert((*tt)->dbs);
    (*tt)->dbscap = 10;
    (*tt)->dbscount = 0;

    (*tt)->txn = txn;
    (*tt)->cur = NULL;
    (*tt)->fin_db = internal_fin_db_locked;
    (*tt)->open_db = internal_get_open_db_locked;

    // assume the tt will always be returned with fin
    // negate fin if wanting to keep the txn
    actions = rdwr;
    if(actions == TRASH_WR_TXN) {
        actions |= TRASH_TXN_COMMIT;
    }
    (*tt)->actions = actions;
}

static void internal_create_trash_cursor(TrashCursor **tc, struct OpenDb *db, enum RWTxn rw) {
    *tc = (TrashCursor *)malloc(sizeof(TrashCursor));
    (*tc)->db = db;
    (*tc)->rw = rw;
}