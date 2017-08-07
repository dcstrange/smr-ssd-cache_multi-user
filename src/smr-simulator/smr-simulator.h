#ifndef SMR_SSD_CACHE_SMR_SIMULATOR_H
#define SMR_SSD_CACHE_SMR_SIMULATOR_H

#include "global.h"
#include "statusDef.h"

#define DEBUG 0
/* ---------------------------smr simulator---------------------------- */
#include <pthread.h>

typedef struct
{
    off_t offset;
} DespTag;

typedef struct
{
    DespTag tag;
    long    despId;
    long    pre_useId;
    long    next_useId;
    long    next_freeId;
} FIFODesc;

typedef struct SSDHashBucket
{
        DespTag	hash_key;
        long    despId;
        struct SSDHashBucket *next_item;
} SSDHashBucket;

typedef struct
{
	unsigned long	n_used;
	long		    first_useId;		// Head of list of used
	long		    last_useId;		// Tail of list of used
	long            first_freeId;
	long            last_freeId;
} FIFOCtrl;

extern FIFODesc		*ssd_descriptors;
extern char             *ssd_blocks;
extern FIFOCtrl *global_fifo_ctrl;
extern SSDHashBucket	*ssd_hashtable;

//#define GetSSDblockFromId(despId) ((void *) (ssd_blocks + ((long) (despId)) * SSD_SIZE))
#define GetSSDHashBucket(hash_code) ((SSDHashBucket *) (ssd_hashtable + (unsigned long) (hash_code)))

extern void initFIFOCache();
extern int smrread(int smr_fd, char* buffer, size_t size, off_t offset);
extern int smrwrite(int smr_fd, char* buffer, size_t size, off_t offset);
extern void PrintSimulatorStatistic();

#endif
