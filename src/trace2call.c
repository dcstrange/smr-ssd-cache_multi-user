#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>

#include "global.h"
#include "statusDef.h"

#include "timerUtils.h"
#include "cache.h"
#include "strategy/lru.h"
#include "trace2call.h"
#include "report.h"

extern struct RuntimeSTAT* STT;
#define REPORT_INTERVAL 10000

static void reportCurInfo();
static void report_ontime();
static void resetStatics();

static timeval  tv_trace_start, tv_trace_end;
static double time_trace;

/** single request statistic information **/
static timeval          tv_req_start, tv_req_stop;
static microsecond_t    msec_req;
extern microsecond_t    msec_r_hdd,msec_w_hdd,msec_r_ssd,msec_w_ssd;
extern int IsHit;
char logbuf[512];

 
void
trace_to_iocall(char *trace_file_path, int isWriteOnly,off_t startLBA)
{
    char		action;
    off_t		offset;
    char       *ssd_buffer;
    int	        returnCode;
    int         isFullSSDcache = 0;
#ifdef CG_THROTTLE
    static char* cgbuf;
    int returncode = posix_memalign(&cgbuf, 512, 4096);
#endif // CG_THROTTLE

    FILE *trace;
    if ((trace = fopen(trace_file_path, "rt")) == NULL)
    {
        error("Failed to open the trace file!\n");
        exit(1);
    }

    returnCode = posix_memalign(&ssd_buffer, 1024, 16*sizeof(char) * BLCKSZ);
    if (returnCode < 0)
    {
        error("posix memalign error\n");
        free(ssd_buffer);
        exit(-1);
    }
    int i;
    for (i = 0; i < 16 * BLCKSZ; i++)
    {
        ssd_buffer[i] = '1';
    }

    _TimerLap(&tv_trace_start);
    static int req_cnt = 0;

    while (!feof(trace))
    {

        //        if(feof(trace))
//            fseek(trace,0,SEEK_SET);

#ifdef CG_THROTTLE
        if(pwrite(ram_fd,cgbuf,1024,0) <= 0)
        {
            printf("write ramdisk error:%d\n",errno);
            exit(1);
        }
#endif // CG_THROTTLE

        returnCode = fscanf(trace, "%c %d %lu\n", &action, &i, &offset);
        if (returnCode < 0)
        {
            error("error while reading trace file.");
            break;
        }
        offset = (offset + startLBA) * BLCKSZ;

        if(!isFullSSDcache && (STT->flush_clean_blocks + STT->flush_hdd_blocks) > 0)
        {
            reportCurInfo();
            resetStatics();        // Because we do not care about the statistic while the process of filling SSD cache.
            isFullSSDcache = 1;
        }

#ifdef LOG_SINGLE_REQ
        _TimerLap(&tv_req_start);
#endif // TIMER_SINGLE_REQ
        if (action == ACT_WRITE) // Write = 1
        {
            STT->reqcnt_w++;
            write_block(offset, ssd_buffer);
        }
        else if (!isWriteOnly && action == ACT_READ)    // read = 9
        {
            STT->reqcnt_r++;
            read_block(offset,ssd_buffer);
        }
	else if (action != ACT_READ)
	{
	    printf("Trace file gets a wrong result: action = %c.\n",action);
	    exit(-1);
	}
#ifdef LOG_SINGLE_REQ
        _TimerLap(&tv_req_stop);
        msec_req = TimerInterval_MICRO(&tv_req_start,&tv_req_stop);
        /*
            print log
            format:
            <req_id, r/w, ishit, time cost for: one request, read_ssd, write_ssd, read_smr, write_smr>
        */
        //sprintf(logbuf,"%lu,%c,%d,%ld,%ld,%ld,%ld,%ld\n",STT->reqcnt_s,action,IsHit,msec_req,msec_r_ssd,msec_w_ssd,msec_r_hdd,msec_w_hdd);
       // WriteLog(logbuf);
        msec_r_ssd = msec_w_ssd = msec_r_hdd = msec_w_hdd = 0;
#endif // TIMER_SINGLE_REQ

        if (++STT->reqcnt_s % REPORT_INTERVAL == 0)
        {
            report_ontime();
        }


        //ResizeCacheUsage();
    }

    _TimerLap(&tv_trace_end);
    time_trace = Mirco2Sec(TimerInterval_MICRO(&tv_trace_start,&tv_trace_end));
    reportCurInfo();
//    free(ssd_buffer);
//    fclose(trace);
}

static void reportCurInfo()
{
    printf(" totalreqNum:%lu\n read_req_count: %lu\n write_req_count: %lu\n",
           STT->reqcnt_s,STT->reqcnt_r,STT->reqcnt_w);

    printf(" hit num:%lu\n hitnum_r:%lu\n hitnum_w:%lu\n",
           STT->hitnum_s,STT->hitnum_r,STT->hitnum_w);

    printf(" read_ssd_blocks:%lu\n flush_ssd_blocks:%lu\n read_hdd_blocks:%lu\n flush_hdd_blocks:%lu\n flush_clean_blocks:%lu\n",
           STT->load_ssd_blocks, STT->flush_ssd_blocks, STT->load_hdd_blocks, STT->flush_hdd_blocks, STT->flush_clean_blocks);

//    printf(" hash_miss:%lu\n hashmiss_read:%lu\n hashmiss_write:%lu\n",
//           STT->hashmiss_sum, STT->hashmiss_read, STT->hashmiss_write);

    printf(" total run time (s) : %lf\n time_read_ssd : %lf\n time_write_ssd : %lf\n time_read_smr : %lf\n time_write_smr : %lf\n",
           time_trace, STT->time_read_ssd, STT->time_write_ssd, STT->time_read_hdd, STT->time_write_hdd);
    printf("Batch flush HDD time:%lu\n",msec_bw_hdd);
}

static void report_ontime()
{
//    _TimerLap(&tv_checkpoint);
//    double timecost = Mirco2Sec(TimerInterval_SECOND(&tv_trace_start,&tv_checkpoint));

//     printf("totalreq:%lu, readreq:%lu, hit:%lu, readhit:%lu, flush_ssd_blk:%lu flush_hdd_blk:%lu, hashmiss:%lu, readhassmiss:%lu writehassmiss:%lu\n",
//           STT->reqcnt_s,STT->reqcnt_r, STT->hitnum_s, STT->hitnum_r, STT->flush_ssd_blocks, STT->flush_hdd_blocks, STT->hashmiss_sum, STT->hashmiss_read, STT->hashmiss_write);
        printf("totalreq:%lu, readreq:%lu, hit:%lu, readhit:%lu, flush_ssd_blk:%lu flush_hdd_blk:%lu\n",
           STT->reqcnt_s,STT->reqcnt_r, STT->hitnum_s, STT->hitnum_r, STT->flush_ssd_blocks, STT->flush_hdd_blocks);
}

static void resetStatics()
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
    msec_bw_hdd = 0;
}

