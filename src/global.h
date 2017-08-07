#ifndef _GLOBAL_H
#define _GLOBAL_H 1

#include <sys/types.h>

#include "statusDef.h"
struct RuntimeSTAT
{
    /** This user basic info */
    unsigned int batchId;
    unsigned int userId;
    unsigned int traceId;
    unsigned long startLBA;
    unsigned int isWriteOnly;

    /** Runtime strategy refered parameter **/
    //union StratetyUnion strategyRef;

    /** Runtime Statistic **/
    blkcnt_t cacheLimit;
    blkcnt_t cacheUsage;

    blkcnt_t reqcnt_s;
    blkcnt_t reqcnt_r;
    blkcnt_t reqcnt_w;

    blkcnt_t hitnum_s;
    blkcnt_t hitnum_r;
    blkcnt_t hitnum_w;

    blkcnt_t load_ssd_blocks;
    blkcnt_t load_hdd_blocks;
    blkcnt_t flush_hdd_blocks;
    blkcnt_t flush_ssd_blocks;
    blkcnt_t flush_clean_blocks;

    double time_read_ssd;
    double time_read_hdd;
    double time_write_ssd;
    double time_write_hdd;

    blksize_t hashmiss_sum;
    blksize_t hashmiss_read;
    blksize_t hashmiss_write;
};

typedef enum
{
//    CLOCK = 0,
    LRU,
    LRUofBand,
//    Most,
//    Most_Dirty,
//    SCAN,
//    CMR,
//    SMR,
//    WA,
//    MaxCold,
//    MaxAll,
//    AvgBandHot,
//    HotDivSize,
    /** add for multiuser **/
    LRU_global,
    LRU_private,
    LRU_batch,
    PORE
}SSDEvictionStrategy;

/** This user basic info */
extern int BatchId;
extern int UserId;
extern int TraceId;
extern off_t StartLBA;
extern int WriteOnly;
extern SSDEvictionStrategy EvictStrategy;
extern unsigned long Param1;
extern unsigned long Param2;
extern int BatchSize;

/** All users basic setup **/
extern blkcnt_t NBLOCK_SSD_CACHE;
extern blkcnt_t NTABLE_SSD_CACHE;
extern blkcnt_t SSD_BUFFER_SIZE;
extern blkcnt_t NBLOCK_SMR_FIFO;
//extern blkcnt_t NSMRBands;		// 194180*(18MB+36MB)/2~5TB
//extern blkcnt_t NSMRBlocks;		// 2621952*8KB~20GB
//extern blkcnt_t NSSDs;
extern blkcnt_t NSSDTables;
extern blkcnt_t NBANDTables;
extern blkcnt_t SSD_SIZE;
extern blkcnt_t BLCKSZ;
extern blkcnt_t NZONES;
extern blkcnt_t ZONESZ;
extern blkcnt_t WRITEAMPLIFICATION;
extern blkcnt_t NCOLDBAND;
extern char smr_device[];
extern char ssd_device[];
extern char ram_device[1024];

extern int BandOrBlock;

/*Block = 0, Band=1*/
extern int hdd_fd;
extern int ssd_fd;
extern int ram_fd;
extern struct RuntimeSTAT* STT;

/** Shared memory variable names **/
extern const char* SHM_SSDBUF_STRATEGY_CTRL;
extern const char* SHM_SSDBUF_STRATEGY_DESP;

extern const char* SHM_SSDBUF_DESP_CTRL;
extern const char* SHM_SSDBUF_DESPS;

extern const char* SHM_SSDBUF_HASHTABLE_CTRL;
extern const char* SHM_SSDBUF_HASHTABLE;
extern const char* SHM_SSDBUF_HASHDESPS;
extern const char* SHM_PROCESS_REQ_LOCK;

extern const char* PATH_LOG;
#endif
