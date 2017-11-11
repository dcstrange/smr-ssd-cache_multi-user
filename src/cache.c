#include <stdio.h>
#include <stdlib.h>
#include <memory.h>
#include <unistd.h>

#include "timerUtils.h"
#include "cache.h"
#include "hashtable_utils.h"

#include "strategies.h"

#include "smr-simulator/smr-simulator.h"
#include "smr-simulator/simulator_logfifo.h"

#include "shmlib.h"
#include "report.h"

SSDBufDespCtrl*     ssd_buf_desp_ctrl;
SSDBufDesp*         ssd_buf_desps;


static int          init_SSDDescriptorBuffer();
static int          init_StatisticObj();
static void         flushSSDBuffer(SSDBufDesp * ssd_buf_hdr);
static SSDBufDesp*  allocSSDBuf(SSDBufTag ssd_buf_tag, bool * found, int alloc4What);
static SSDBufDesp*  getAFreeSSDBuf();

static int          initStrategySSDBuffer();
static long         Strategy_Desp_LogOut();
static int          Strategy_Desp_HitIn(SSDBufDesp* desp);
static int         Strategy_Desp_LogIn(SSDBufDesp* desp);
//#define isSamebuf(SSDBufTag tag1, SSDBufTag tag2) (tag1 == tag2)
#define CopySSDBufTag(objectTag,sourceTag) (objectTag = sourceTag)

void                _LOCK(pthread_mutex_t* lock);
void                _UNLOCK(pthread_mutex_t* lock);

/* stopwatch */
static timeval tv_start, tv_stop;
static timeval tv_bastart, tv_bastop;

int IsHit;
microsecond_t msec_r_hdd,msec_w_hdd,msec_r_ssd,msec_w_ssd,msec_bw_hdd=0;

/* Device I/O operation with Timer */
static int dev_pread(int fd, void* buf,size_t nbytes,off_t offset);
static int dev_pwrite(int fd, void* buf,size_t nbytes,off_t offset);
static int dev_simu_read(void* buf,size_t nbytes,off_t offset);
static int dev_simu_write(void* buf,size_t nbytes,off_t offset);

static char* ssd_buffer;

extern struct RuntimeSTAT* STT;
extern struct InitUsrInfo UsrInfo;


/*
 * init buffer hash table, strategy_control, buffer, work_mem
 */
void
initSSD()
{
    int r_initdesp          =   init_SSDDescriptorBuffer();
    int r_initstrategybuf   =   initStrategySSDBuffer();
    int r_initbuftb         =   HashTab_Init();
    int r_initstt           =   init_StatisticObj();
    printf("init_Strategy: %d, init_table: %d, init_desp: %d, inti_Stt: %d\n",r_initstrategybuf, r_initbuftb, r_initdesp, r_initstt);

    if(r_initdesp==-1 || r_initstrategybuf==-1 || r_initbuftb==-1 || r_initstt==-1)
        exit(-1);
    int returnCode;
    returnCode = posix_memalign(&ssd_buffer, 512, sizeof(char) * BLCKSZ);
    if (returnCode < 0)
    {
        printf("[ERROR] flushSSDBuffer():--------posix memalign\n");
        free(ssd_buffer);
        exit(-1);
    }
}

static int
init_SSDDescriptorBuffer()
{
    int stat = multi_SHM_lock_n_check("LOCK_SSDBUF_DESP");
    if(stat == 0)
    {
        ssd_buf_desp_ctrl = (SSDBufDespCtrl*)multi_SHM_alloc(SHM_SSDBUF_DESP_CTRL,sizeof(SSDBufDespCtrl));
        ssd_buf_desps = (SSDBufDesp *)multi_SHM_alloc(SHM_SSDBUF_DESPS,sizeof(SSDBufDesp) * NBLOCK_SSD_CACHE);

        ssd_buf_desp_ctrl->n_usedssd = 0;
        ssd_buf_desp_ctrl->first_freessd = 0;
        multi_SHM_mutex_init(&ssd_buf_desp_ctrl->lock);

        long i;
        SSDBufDesp  *ssd_buf_hdr = ssd_buf_desps;
        for (i = 0; i < NBLOCK_SSD_CACHE; ssd_buf_hdr++, i++)
        {
            ssd_buf_hdr->serial_id = i;
            ssd_buf_hdr->ssd_buf_id = i;
            ssd_buf_hdr->ssd_buf_flag = 0;
            ssd_buf_hdr->next_freessd = i + 1;
            multi_SHM_mutex_init(&ssd_buf_hdr->lock);
        }
        ssd_buf_desps[NBLOCK_SSD_CACHE - 1].next_freessd = -1;
    }
    else
    {
        ssd_buf_desp_ctrl = (SSDBufDespCtrl *)multi_SHM_get(SHM_SSDBUF_DESP_CTRL,sizeof(SSDBufDespCtrl));
        ssd_buf_desps = (SSDBufDesp *)multi_SHM_get(SHM_SSDBUF_DESPS,sizeof(SSDBufDesp) * NBLOCK_SSD_CACHE);
    }
    multi_SHM_unlock("LOCK_SSDBUF_DESP");
    return stat;
}

static int
init_StatisticObj()
{
    STT->hitnum_s = 0;
    STT->hitnum_r = 0;
    STT->hitnum_w = 0;
    STT->load_ssd_blocks = 0;
    STT->flush_ssd_blocks = 0;
    STT->load_hdd_blocks = 0;
    STT->flush_hdd_blocks = 0;
    STT->flush_clean_blocks = 0;

    STT->time_read_hdd = 0.0;
    STT->time_write_hdd = 0.0;
    STT->time_read_ssd = 0.0;
    STT->time_write_ssd = 0.0;
    STT->hashmiss_sum = 0;
    STT->hashmiss_read = 0;
    STT->hashmiss_write = 0;
    return 0;
}

static void
flushSSDBuffer(SSDBufDesp * ssd_buf_hdr)
{
    if ((ssd_buf_hdr->ssd_buf_flag & SSD_BUF_DIRTY) == 0)
    {
        STT->flush_clean_blocks++;
        return;
    }

    dev_pread(ssd_fd, ssd_buffer, SSD_BUFFER_SIZE, ssd_buf_hdr->ssd_buf_id * SSD_BUFFER_SIZE);
    msec_r_ssd = TimerInterval_MICRO(&tv_start,&tv_stop);
    STT->time_read_ssd += Mirco2Sec(msec_r_ssd);
    STT->load_ssd_blocks++;
#ifdef SIMULATION
    dev_simu_write(ssd_buffer, SSD_BUFFER_SIZE, ssd_buf_hdr->ssd_buf_tag.offset);
#else
    dev_pwrite(hdd_fd, ssd_buffer, SSD_BUFFER_SIZE, ssd_buf_hdr->ssd_buf_tag.offset);
#endif
    msec_w_hdd = TimerInterval_MICRO(&tv_start,&tv_stop);
    STT->time_write_hdd += Mirco2Sec(msec_w_hdd);
    STT->flush_hdd_blocks++;
}

int ResizeCacheUsage()
{
    blksize_t needEvictCnt = STT->cacheUsage - STT->cacheLimit;
    if(needEvictCnt <= 0)
        return 0;

    while(needEvictCnt-- > 0)
    {
        long unloadId = Strategy_Desp_LogOut();
        SSDBufDesp* ssd_buf_hdr = &ssd_buf_desps[unloadId];

        // TODO Flush
        _LOCK(&ssd_buf_hdr->lock);
        flushSSDBuffer(ssd_buf_hdr);

        ssd_buf_hdr->ssd_buf_flag &= ~(SSD_BUF_VALID | SSD_BUF_DIRTY);
        ssd_buf_hdr->ssd_buf_tag.offset = -1;
        _UNLOCK(&ssd_buf_hdr->lock);

        _LOCK(&ssd_buf_desp_ctrl->lock);
        ssd_buf_hdr->next_freessd = ssd_buf_desp_ctrl->first_freessd;
        ssd_buf_desp_ctrl->first_freessd = ssd_buf_hdr->serial_id;
        _UNLOCK(&ssd_buf_desp_ctrl->lock);
    }
    return 0;
}

static void flagOp(SSDBufDesp * ssd_buf_hdr, int opType)
{
    ssd_buf_hdr->ssd_buf_flag |= SSD_BUF_VALID;
    if(opType){
        // write operation
        ssd_buf_hdr->ssd_buf_flag |= SSD_BUF_DIRTY;
    }

}

static SSDBufDesp*
allocSSDBuf(SSDBufTag ssd_buf_tag, bool * found, int alloc4What)
{
    /* Lookup if already cached. */
    SSDBufDesp      *ssd_buf_hdr; //returned value.
    unsigned long   ssd_buf_hash = HashTab_GetHashCode(ssd_buf_tag);
    long            ssd_buf_id = HashTab_Lookup(ssd_buf_tag, ssd_buf_hash);

    /* Cache HIT IN */
    if (ssd_buf_id >= 0)
    {
        ssd_buf_hdr = &ssd_buf_desps[ssd_buf_id];
        _LOCK(&ssd_buf_hdr->lock);
 //       if(isSamebuf(&ssd_buf_hdr->ssd_buf_tag,ssd_buf_tag))
 //       {
        flagOp(ssd_buf_hdr,alloc4What);
        Strategy_Desp_HitIn(ssd_buf_hdr); //need lock

        STT->hitnum_s++;
        *found = 1;
        return ssd_buf_hdr;
//        }
//        else
//        {
//            _UNLOCK(&ssd_buf_hdr->lock);
//            /** passive delete hash item, which corresponding cache buf has been evicted early **/
//            HashTab_Delete(ssd_buf_tag,ssd_buf_hash);
//            STT->hashmiss_sum++;
//            if(alloc4What == 1)	// alloc for write
//                STT->hashmiss_write++;
//            else		//alloc for read
//                STT->hashmiss_read++;
//        }
    }

    /* Cache MISS */
    *found = 0;

    _LOCK(&ssd_buf_desp_ctrl->lock);
    ssd_buf_hdr = getAFreeSSDBuf();
    _UNLOCK(&ssd_buf_desp_ctrl->lock);

    if (ssd_buf_hdr != NULL)
    {
        _LOCK(&ssd_buf_hdr->lock);
        // if there is free SSD buffer.
    }
    else
    {
        /** When there is NO free SSD space for cache **/
        // TODO Choose a buffer by strategy/
#ifdef _LRU_BATCH_H_
    static long unloads[20000];
    static long intervaltime;
    static char timestr[50];
    Unload_Buf_LRU_batch(unloads,BatchSize);
	int i = 0;

//	while(i<BatchSize)
//	{
//		printf("%d ",unloads[i]);
//		i++;
//	}
//	i=0;
        _TimerLap(&tv_bastart);
        while(i<BatchSize)
        {
            ssd_buf_hdr = &ssd_buf_desps[unloads[i]];
            _LOCK(&ssd_buf_hdr->lock);
            flushSSDBuffer(ssd_buf_hdr);
            ssd_buf_hdr->ssd_buf_flag &= ~(SSD_BUF_VALID | SSD_BUF_DIRTY);
            ssd_buf_hdr->ssd_buf_tag.offset = -1;

            //push into free ssd stack
            ssd_buf_hdr->next_freessd = ssd_buf_desp_ctrl->first_freessd;
            ssd_buf_desp_ctrl->first_freessd = ssd_buf_hdr->serial_id;
            i++;
            _UNLOCK(&ssd_buf_hdr->lock);
        }
        _TimerLap(&tv_bastop);
	intervaltime = TimerInterval_MICRO(&tv_bastart,&tv_bastop);
        msec_bw_hdd += intervaltime;
	sprintf(timestr,"%lu\n",intervaltime);
	WriteLog(timestr);
	_LOCK(&ssd_buf_desp_ctrl->lock);
        ssd_buf_hdr = getAFreeSSDBuf();
        _UNLOCK(&ssd_buf_desp_ctrl->lock);
        _LOCK(&ssd_buf_hdr->lock);
#else
        long out_despId = Strategy_Desp_LogOut(); //need look
        ssd_buf_hdr = &ssd_buf_desps[out_despId];
        _LOCK(&ssd_buf_hdr->lock);
        // Clear Hashtable item.
        SSDBufTag oldtag = ssd_buf_hdr->ssd_buf_tag;
        unsigned long hash = HashTab_GetHashCode(oldtag);
        HashTab_Delete(oldtag,hash);
        // TODO Flush
        flushSSDBuffer(ssd_buf_hdr);
        ssd_buf_hdr->ssd_buf_flag &= ~(SSD_BUF_VALID | SSD_BUF_DIRTY);

#endif // _LRU_BATCH_H_

    }
    flagOp(ssd_buf_hdr,alloc4What);
    CopySSDBufTag(ssd_buf_hdr->ssd_buf_tag,ssd_buf_tag);

    HashTab_Insert(ssd_buf_tag, ssd_buf_hash, ssd_buf_hdr->serial_id);
    Strategy_Desp_LogIn(ssd_buf_hdr);

    return ssd_buf_hdr;
}

static int
initStrategySSDBuffer()
{
    switch(EvictStrategy)
    {
        case LRU_private:       return initSSDBufferFor_LRU_private();
//        case Most:              return initSSDBufferForMost();
        case PORE:              return InitPORE();
        case PORE_PLUS:         return InitPORE_plus();

    }
    return -1;
}

static long
Strategy_Desp_LogOut()
{
    STT->cacheUsage--;
    switch(EvictStrategy)
    {
//        case LRU_global:        return Unload_LRUBuf();
        case LRU_private:       return Unload_Buf_LRU_private();
 //       case Most:              return LogOutDesp_most();
        case PORE:              return LogOutDesp_pore();
        case PORE_PLUS:        return LogOutDesp_pore_plus();
    }
    return -1;
}

static int
Strategy_Desp_HitIn(SSDBufDesp* desp)
{
    switch(EvictStrategy)
    {
//        case LRU_global:        return hitInLRUBuffer(desp->serial_id);
        case LRU_private:       return hitInBuffer_LRU_private(desp->serial_id);
//        case LRU_batch:         return hitInBuffer_LRU_batch(desp->serial_id);
//        case Most:              return HitMostBuffer();
        case PORE:              return HitPoreBuffer(desp->serial_id, desp->ssd_buf_flag);
        case PORE_PLUS:         return HitPoreBuffer_plus(desp->serial_id, desp->ssd_buf_flag);
    }
    return -1;
}

static int
Strategy_Desp_LogIn(SSDBufDesp* desp)
{
    STT->cacheUsage++;
    switch(EvictStrategy)
    {
//        case LRU_global:        return insertLRUBuffer(serial_id);
        case LRU_private:       return insertBuffer_LRU_private(desp->serial_id);
//        case LRU_batch:         return insertBuffer_LRU_batch(serial_id);
//        case Most:              return LogInMostBuffer(desp->serial_id,desp->ssd_buf_tag);
        case PORE:              return LogInPoreBuffer(desp->serial_id, desp->ssd_buf_tag, desp->ssd_buf_flag);
        case PORE_PLUS:         return LogInPoreBuffer_plus(desp->serial_id, desp->ssd_buf_tag, desp->ssd_buf_flag);
    }
    return -1;
}
/*
 * read--return the buf_id of buffer according to buf_tag
 */

void
read_block(off_t offset, char *ssd_buffer)
{
#ifdef NO_CACHE
    #ifdef SIMULATION
    dev_simu_read(ssd_buffer, SSD_BUFFER_SIZE, offset);
    #else
    dev_pread(hdd_fd, ssd_buffer, BLCKSZ, offset);
    #endif // SIMULATION
    msec_r_hdd = TimerInterval_MICRO(&tv_start,&tv_stop);
    STT->time_read_hdd += Mirco2Sec(msec_r_hdd);
    STT->load_hdd_blocks++;
    return;
#else
    bool found = 0;
    static SSDBufTag ssd_buf_tag;
    static SSDBufDesp* ssd_buf_hdr;

    ssd_buf_tag.offset = offset;
    if (DEBUG)
        printf("[INFO] read():-------offset=%lu\n", offset);

    ssd_buf_hdr = allocSSDBuf(ssd_buf_tag, &found, 0);

    IsHit = found;
    if (found)
    {
        dev_pread(ssd_fd, ssd_buffer, SSD_BUFFER_SIZE, ssd_buf_hdr->ssd_buf_id * SSD_BUFFER_SIZE);
        msec_r_ssd = TimerInterval_MICRO(&tv_start,&tv_stop);

        STT->hitnum_r++;
        STT->time_read_ssd += Mirco2Sec(msec_r_ssd);
        STT->load_ssd_blocks++;
    }
    else
    {
    #ifdef SIMULATION
        dev_simu_read(ssd_buffer, SSD_BUFFER_SIZE, offset);
    #else
        dev_pread(hdd_fd, ssd_buffer, SSD_BUFFER_SIZE, offset);
    #endif // SIMULATION
        msec_r_hdd = TimerInterval_MICRO(&tv_start,&tv_stop);
        STT->time_read_hdd += Mirco2Sec(msec_r_hdd);
        STT->load_hdd_blocks++;

        dev_pwrite(ssd_fd, ssd_buffer, SSD_BUFFER_SIZE, ssd_buf_hdr->ssd_buf_id * SSD_BUFFER_SIZE);
        msec_w_ssd = TimerInterval_MICRO(&tv_start,&tv_stop);
        STT->time_write_ssd += Mirco2Sec(msec_w_ssd);
        STT->flush_ssd_blocks++;
    }
    ssd_buf_hdr->ssd_buf_flag &= ~SSD_BUF_DIRTY;
    ssd_buf_hdr->ssd_buf_flag |= SSD_BUF_VALID;

    _UNLOCK(&ssd_buf_hdr->lock);
#endif // NO_CACHE
}

/*
 * write--return the buf_id of buffer according to buf_tag
 */
void
write_block(off_t offset, char *ssd_buffer)
{
#ifdef NO_CACHE
    #ifdef SIMULATION
    dev_simu_write(ssd_buffer, BLCKSZ, offset);
    #else
    dev_pwrite(hdd_fd, ssd_buffer, BLCKSZ, offset);
    #endif // SIMULATION
    //IO by no cache.

    msec_w_hdd = TimerInterval_MICRO(&tv_start,&tv_stop);
    STT->time_write_hdd += Mirco2Sec(msec_w_hdd);
    STT->flush_hdd_blocks++;
    return;
#else
    bool	found;

    static SSDBufTag ssd_buf_tag;
    static SSDBufDesp   *ssd_buf_hdr;

    ssd_buf_tag.offset = offset;
    ssd_buf_hdr = allocSSDBuf(ssd_buf_tag, &found, 1);

    IsHit = found;
    STT->hitnum_w += found;

    dev_pwrite(ssd_fd, ssd_buffer, SSD_BUFFER_SIZE, ssd_buf_hdr->ssd_buf_id * SSD_BUFFER_SIZE);
    msec_w_ssd = TimerInterval_MICRO(&tv_start,&tv_stop);
    STT->time_write_ssd += Mirco2Sec(msec_w_ssd);
    STT->flush_ssd_blocks++ ;

    _UNLOCK(&ssd_buf_hdr->lock);
#endif // NO_CAHCE


}

/******************
**** Utilities*****
*******************/

static int dev_pread(int fd, void* buf,size_t nbytes,off_t offset)
{
#ifdef NO_REAL_DISK_IO
    return nbytes;
#else
    int r;
    _TimerLap(&tv_start);
    r = pread(fd,buf,nbytes,offset);
    _TimerLap(&tv_stop);
    if (r < 0)
    {
        printf("[ERROR] read():-------read from device: fd=%d, errorcode=%d, offset=%lu\n", fd, r, offset);
        exit(-1);
    }
    return r;
#endif
}

static int dev_pwrite(int fd, void* buf,size_t nbytes,off_t offset)
{
#ifdef NO_REAL_DISK_IO
    return nbytes;
#else
    int w;
    _TimerLap(&tv_start);
    w = pwrite(fd,buf,nbytes,offset);
    _TimerLap(&tv_stop);
    if (w < 0)
    {
        printf("[ERROR] read():-------write to device: fd=%d, errorcode=%d, offset=%lu\n", fd, w, offset);
        exit(-1);
    }
    return w;
#endif
}

static int dev_simu_write(void* buf,size_t nbytes,off_t offset)
{
    int w;
    _TimerLap(&tv_start);
    w = simu_smr_write(buf,nbytes,offset);
    _TimerLap(&tv_stop);
    return w;
}

static int dev_simu_read(void* buf,size_t nbytes,off_t offset)
{
    int r;
    _TimerLap(&tv_start);
    r = simu_smr_read(buf,nbytes,offset);
    _TimerLap(&tv_stop);
    return r;
}

static SSDBufDesp*
getAFreeSSDBuf()
{
    if(ssd_buf_desp_ctrl->first_freessd < 0)
        return NULL;
    SSDBufDesp* ssd_buf_hdr = &ssd_buf_desps[ssd_buf_desp_ctrl->first_freessd];
    ssd_buf_desp_ctrl->first_freessd = ssd_buf_hdr->next_freessd;
    ssd_buf_hdr->next_freessd = -1;
    ssd_buf_desp_ctrl->n_usedssd++;
    return ssd_buf_hdr;
}

void
_LOCK(pthread_mutex_t* lock)
{
#ifdef MULTIUSER
    SHM_mutex_lock(lock);
#endif // MULTIUSER
}

void
_UNLOCK(pthread_mutex_t* lock)
{
#ifdef MULTIUSER
    SHM_mutex_unlock(lock);
#endif // MULTIUSER
}
