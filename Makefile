CC	= gcc
AR	= ar
W = -W -Wall -g
LDFLAGS = -llmdb

BIN_DIR = bin
LIB_DIR = lib
TARGET = ligmadb.a

TESTS = dbtest dbmtest

TEST_MODE ?= 0

ifeq ($(TEST_MODE), 1)
  CFLAGS += -DTEST
endif

.PHONY: clean lib

all:	lib $(TARGET)

clean:
	rm -rf $(LIB_DIR) $(BIN_DIR) $(TARGET)	

bin:
	@mkdir -p $(BIN_DIR)

lib:
	@mkdir -p $(LIB_DIR)

libutils.a:
	$(MAKE) -C c-utils
	cp -r c-utils/lib/libutils.a $(LIB_DIR)/

test:	bin lib	$(TESTS)

dbtest:		dbtest.o	$(TARGET)	libutils.a
	$(CC) $(W) -o $(BIN_DIR)/$@ $(addprefix $(LIB_DIR)/, $^) $(LDFLAGS)

dbmtest:	dbmtest.o	$(TARGET)	libutils.a
	$(CC) $(W) -o $(BIN_DIR)/$@ $(addprefix $(LIB_DIR)/, $^) $(LDFLAGS)

%.o:	%.c
	$(CC) $(CFLAGS) $(W) -c $< -o $(LIB_DIR)/$@

%.o:	test/%.c
	$(CC) $(CFLAGS) $(W) -c $< -o $(LIB_DIR)/$@

$(TARGET): libutils.a db.o
	$(AR) rs $(LIB_DIR)/$@ $(addprefix $(LIB_DIR)/, $^)