#include "global.h"

/** This user basic info */
pid_t Fork_Pid = 0; /* Default 0. If is a HRC process, this must be large than 0 */
int BatchId;
int UserId;
int TraceId;
off_t StartLBA;
int WriteOnly;
int BatchSize;
SSDEvictionStrategy EvictStrategy;
long PeriodLenth;
unsigned long Param1;
unsigned long Param2;

/** All users basic setup **/           /** NEED TO BE '#DEFINE' **/
blksize_t NBLOCK_MAX_CACHE_SIZE;
blksize_t NBLOCK_SSD_CACHE;
blksize_t NTABLE_SSD_CACHE;
blksize_t SSD_BUFFER_SIZE = 4096;
blksize_t NBLOCK_SMR_FIFO;
//blksize_t NSMRBlocks = 2621952;		// 2621952*8KB~20GB
//blksize_t NSSDs;
//blksize_t NSSDTables;
blksize_t NBANDTables = 2621952;
blksize_t SSD_SIZE = 4096;
blksize_t BLKSZ = 4096;
blkcnt_t  NZONES = 466034;/* size = 8TB */ //194180;    // NZONES * ZONESZ =
blksize_t ZONESZ = 18 * 1024 * 1024;//18MB    // Unit: Byte.

char simu_smr_fifo_device[] = "/dev/sdc1";
char simu_smr_smr_device[] = "/dev/sdc2";
char smr_device[] = "/dev/md0"; // /dev/sdc";
char ssd_device[] = "/dev/memdiska";//"/mnt/ramdisk/ramdisk";//"/dev/memdiska";// "/mnt/ssd/ssd";
char ram_device[1024];

int BandOrBlock;

/*Block = 0, Band=1*/
int hdd_fd;
int ssd_fd;
int ram_fd;
struct RuntimeSTAT* STT;

/** Shared memory variable names **/
char* SHM_SSDBUF_STRATEGY_CTRL = "SHM_SSDBUF_STRATEGY_CTRL";
char* SHM_SSDBUF_STRATEGY_DESP = "SHM_SSDBUF_STRATEGY_DESP";

char* SHM_SSDBUF_DESP_CTRL = "SHM_SSDBUF_DESP_CTRL";
char* SHM_SSDBUF_DESPS = "SHM_SSDBUF_DESPS";

char* SHM_SSDBUF_HASHTABLE_CTRL = "SHM_SSDBUF_HASHTABLE_CTRL";
char* SHM_SSDBUF_HASHTABLE = "SHM_SSDBUF_HASHTABLE";
char* SHM_SSDBUF_HASHDESPS =  "SHM_SSDBUF_HASHDESPS";
char* SHM_PROCESS_REQ_LOCK = "SHM_PROCESS_REQ_LOCK";

char* PATH_LOG = "/home/outputs/logs";

/** Var for T-Switcher **/

/** Pipes for HRC processes **/
#ifdef HRC_PROCS_N
int PipeEnds_of_MAIN[HRC_PROCS_N];
int PipeEnd_of_HRC;
#endif
