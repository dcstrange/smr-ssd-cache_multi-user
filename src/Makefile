CC = gcc -g -g

ifndef SMR_SSD_CACHE_DIR
SMR_SSD_CACHE_DIR = .
endif

CFLAGS += -Wall -pthread
CPPFLAGS += -I$(SMR_SSD_CACHE_DIR) -I$(SMR_SSD_CACHE_DIR)/smr-simulator -I$(SMR_SSD_CACHE_DIR)/strategy

RM = rm -rf
RMSHM = rm -f /dev/shm/*
OBJS = global.o report.o timerUtils.o shmlib.o hashtable_utils.o cache.o trace2call.o main.o losertree4pore.o pore.o lru.o lru_private.o lru_batch.o

all: $(OBJS) smr-ssd-cache
	@echo 'Successfully built smr-ssd-cache...'

smr-ssd-cache:
	$(CC) $(CPPFLAGS) $(CFLAGS) $(OBJS) -lrt -lm -o $@

global.o: global.c
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $?

report.o: report.c
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $?

shmlib.o: shmlib.c
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $?

timerUtils.o: timerUtils.c
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $?

hashtable_utils.o: hashtable_utils.c
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $?

cahce.o: cache.c
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $?

trace2call.o: trace2call.c
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $?

main.o: main.c
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $?

losertree4pore.o: strategy/losertree4pore.c
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $?

pore.o: strategy/pore.c
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $?

clock.o: strategy/clock.c
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $?

lru.o: strategy/lru.c
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $?

lru_private.o: strategy/lru_private.c
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $?

lru_batch.o: strategy/lru_batch.c
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $?

scan.o: strategy/scan.c
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $?

lruofband.o: strategy/lruofband.c
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $?

band_table.o: strategy/band_table.c
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $?

most.o: strategy/most.c
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $?

WA.o: strategy/WA.c
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $?

ssd_buf_table_for_coldmax_history.o: strategy/ssd_buf_table_for_coldmax_history.c
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $?

maxcold.o: strategy/maxcold.c
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $?

clean:
	$(RM) *.o
	$(RM) $(SMR_SSD_CACHE_DIR)/smr-ssd-cache
	$(RMSHM)
