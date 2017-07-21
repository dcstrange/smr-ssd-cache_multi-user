#include <stdio.h>
#include <stdlib.h>
#include <memory.h>
#include <unistd.h>

#include "timerUtils.h"
#include "cache.h"
#include "hashtable_utils.h"
#include "strategy.h"
#include "shmlib.h"
#include "report.h"

SSDBufDespCtrl*     ssd_buf_desp_ctrl;
SSDBufDesp*         ssd_buf_desps;


static int          init_SSDDescriptorBuffer();
static int          init_StatisticObj();
static void         flushSSDBuffer(SSDBufDesp * ssd_buf_hdr);
static SSDBufDesp*  allocSSDBuf(SSDBufferTag *ssd_buf_tag, bool * found, int alloc4What);
static SSDBufDesp*  getAFreeSSDBuf();
static int resizeCacheUsage();

static int          initStrategySSDBuffer();
static long         Strategy_GetUnloadBufID();
static int          Strategy_HitIn(long serial_id);
static void        Strategy_AddBufID(long serial_id);

void                CopySSDBufTag(SSDBufferTag* objectTag, SSDBufferTag* sourceTag);

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
    int stat = SHM_lock_n_check("LOCK_SSDBUF_DESP");
    if(stat == 0)
    {
        ssd_buf_desp_ctrl = (SSDBufDespCtrl*)SHM_alloc(SHM_SSDBUF_DESP_CTRL,sizeof(SSDBufDespCtrl));
        ssd_buf_desps = (SSDBufDesp *)SHM_alloc(SHM_SSDBUF_DESPS,sizeof(SSDBufDesp) * NBLOCK_SSD_CACHE);

        ssd_buf_desp_ctrl->n_usedssd = 0;
        ssd_buf_desp_ctrl->first_freessd = 0;
        SHM_mutex_init(&ssd_buf_desp_ctrl->lock);

        long i;
        SSDBufDesp  *ssd_buf_hdr = ssd_buf_desps;
        for (i = 0; i < NBLOCK_SSD_CACHE; ssd_buf_hdr++, i++)
        {
            ssd_buf_hdr->serial_id = i;
            ssd_buf_hdr->ssd_buf_id = i;
            ssd_buf_hdr->ssd_buf_flag = 0;
            ssd_buf_hdr->next_freessd = i + 1;
            SHM_mutex_init(&ssd_buf_hdr->lock);
        }
        ssd_buf_desps[NBLOCK_SSD_CACHE - 1].next_freessd = -1;
    }
    else
    {
        ssd_buf_desp_ctrl = (SSDBufDespCtrl *)SHM_get(SHM_SSDBUF_DESP_CTRL,sizeof(SSDBufDespCtrl));
        ssd_buf_desps = (SSDBufDesp *)SHM_get(SHM_SSDBUF_DESPS,sizeof(SSDBufDesp) * NBLOCK_SSD_CACHE);
    }
    SHM_unlock("LOCK_SSDBUF_DESP");
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

    dev_pwrite(hdd_fd, ssd_buffer, SSD_BUFFER_SIZE, ssd_buf_hdr->ssd_buf_tag.offset);
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
        long unloadId = Strategy_GetUnloadBufID();
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
}

long unloads[20000];
long intervaltime;
char timestr[50];
static SSDBufDesp*
allocSSDBuf(SSDBufferTag *ssd_buf_tag, bool * found, int alloc4What)
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
        if(isSamebuf(&ssd_buf_hdr->ssd_buf_tag,ssd_buf_tag))
        {
            Strategy_HitIn(ssd_buf_hdr->serial_id); //need lock
            STT->hitnum_s++;
            *found = 1;
            return ssd_buf_hdr;
        }
        else
        {
            _UNLOCK(&ssd_buf_hdr->lock);
            /** passive delete hash item, which corresponding cache buf has been evicted early **/
            HashTab_Delete(ssd_buf_tag,ssd_buf_hash);
            STT->hashmiss_sum++;
            if(alloc4What == 1)	// alloc for write
                STT->hashmiss_write++;
            else		//alloc for read
                STT->hashmiss_read++;
        }
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
        Unload_Buf_LRU_batch(unloads,BatchSize);
	int i = 0;

//	while(i<BatchSize)
//	{
//		printf("%d ",unloads[i]);
//		i++;
//	}
//	i=0;
        _TimerStart(&tv_bastart);
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
        _TimerStop(&tv_bastop);
	intervaltime = TimerInterval_MICRO(&tv_bastart,&tv_bastop);
        msec_bw_hdd += intervaltime;
	sprintf(timestr,"%lu\n",intervaltime);
	WriteLog(timestr);
	_LOCK(&ssd_buf_desp_ctrl->lock);
        ssd_buf_hdr = getAFreeSSDBuf();
        _UNLOCK(&ssd_buf_desp_ctrl->lock);
        _LOCK(&ssd_buf_hdr->lock);
        #else
        long renew_buf = Strategy_GetUnloadBufID(ssd_buf_tag, EvictStrategy); //need look
        ssd_buf_hdr = &ssd_buf_desps[renew_buf];
        _LOCK(&ssd_buf_hdr->lock);

        // TODO Flush
        flushSSDBuffer(ssd_buf_hdr);

        #endif // _LRU_BATCH_H_

    }

    Strategy_AddBufID(ssd_buf_hdr->serial_id);

    ssd_buf_hdr->ssd_buf_flag &= ~(SSD_BUF_VALID | SSD_BUF_DIRTY);
    CopySSDBufTag(&ssd_buf_hdr->ssd_buf_tag,ssd_buf_tag);

    HashTab_Insert(ssd_buf_tag, ssd_buf_hash, ssd_buf_hdr->serial_id);
    return ssd_buf_hdr;
}

static int
initStrategySSDBuffer()
{
    /** Add for multi-user **/
    if (EvictStrategy == LRU_global)
        return initSSDBufferForLRU();
    else if(EvictStrategy == LRU_private)
        return initSSDBufferFor_LRU_private();
    else if(EvictStrategy == LRU_batch)
        return initSSDBufferFor_LRU_batch();
    return -1;
}

static long
Strategy_GetUnloadBufID()
{
    STT->cacheUsage--;
    switch(EvictStrategy)
    {
        case LRU_global:        return Unload_LRUBuf();
        case LRU_private:       return Unload_Buf_LRU_private();
    }
    return -1;
}

static int
Strategy_HitIn(long serial_id)
{
    switch(EvictStrategy)
    {
        case LRU_global:        return hitInLRUBuffer(serial_id);
        case LRU_private:       return hitInBuffer_LRU_private(serial_id);
        case LRU_batch:         return hitInBuffer_LRU_batch(serial_id);
    }
    return -1;
}

static void
Strategy_AddBufID(long serial_id)
{
    STT->cacheUsage++;
    switch(EvictStrategy)
    {
        case LRU_global:        return insertLRUBuffer(serial_id);
        case LRU_private:       return insertBuffer_LRU_private(serial_id);
        case LRU_batch:         return insertBuffer_LRU_batch(serial_id);
    }
}
/*
 * read--return the buf_id of buffer according to buf_tag
 */

void
read_block(off_t offset, char *ssd_buffer)
{
    #ifdef _NO_CACHE_

    #else
    bool found = 0;
    static SSDBufferTag ssd_buf_tag;
    static SSDBufDesp* ssd_buf_hdr;

    ssd_buf_tag.offset = offset;
    if (DEBUG)
        printf("[INFO] read():-------offset=%lu\n", offset);

    ssd_buf_hdr = allocSSDBuf(&ssd_buf_tag, &found, 0);

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
        dev_pread(hdd_fd, ssd_buffer, SSD_BUFFER_SIZE, offset);
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
    #endif // _NO_CACHE_
}

/*
 * write--return the buf_id of buffer according to buf_tag
 */
void
write_block(off_t offset, char *ssd_buffer)
{
    #ifdef _NO_CACHE_
    //IO by no cache.
    #else
    bool	found;

    static SSDBufferTag ssd_buf_tag;
    static SSDBufDesp   *ssd_buf_hdr;

    ssd_buf_tag.offset = offset;
    ssd_buf_hdr = allocSSDBuf(&ssd_buf_tag, &found, 1);

    IsHit = found;
    STT->hitnum_w += found;

    dev_pwrite(ssd_fd, ssd_buffer, SSD_BUFFER_SIZE, ssd_buf_hdr->ssd_buf_id * SSD_BUFFER_SIZE);
    msec_w_ssd = TimerInterval_MICRO(&tv_start,&tv_stop);
    STT->time_write_ssd += Mirco2Sec(msec_w_ssd);
    STT->flush_ssd_blocks++ ;

    ssd_buf_hdr->ssd_buf_flag |= SSD_BUF_VALID | SSD_BUF_DIRTY;
    _UNLOCK(&ssd_buf_hdr->lock);
    #endif // _NO_CAHCE_


}

/******************
**** Utilities*****
*******************/

static int dev_pread(int fd, void* buf,size_t nbytes,off_t offset)
{
    _TimerStart(&tv_start);
    int r = pread(fd,buf,nbytes,offset);
    _TimerStop(&tv_stop);
    if (r < 0)
    {
        printf("[ERROR] read():-------read from device: fd=%d, errorcode=%d, offset=%lu\n", fd, r, offset);
        exit(-1);
    }
    return r;
}

static int dev_pwrite(int fd, void* buf,size_t nbytes,off_t offset)
{
    _TimerStart(&tv_start);
    int w = pwrite(fd,buf,nbytes,offset);
    _TimerStop(&tv_stop);
    if (w < 0)
    {
        printf("[ERROR] read():-------write to device: fd=%d, errorcode=%d, offset=%lu\n", fd, w, offset);
        exit(-1);
    }
    return w;
}

void CopySSDBufTag(SSDBufferTag* objectTag, SSDBufferTag* sourceTag)
{
    objectTag->offset = sourceTag->offset;
}

bool isSamebuf(SSDBufferTag *tag1, SSDBufferTag *tag2)
{
    if (tag1->offset != tag2->offset)
        return 0;
    else return 1;
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
    SHM_mutex_lock(lock);
}

void
_UNLOCK(pthread_mutex_t* lock)
{
    SHM_mutex_unlock(lock);
}
