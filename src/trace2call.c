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
#include "strategy/strategies.h"

//#include "/home/fei/git/Func-Utils/pipelib.h"
#include "hrc.h"
extern struct RuntimeSTAT* STT;
#define REPORT_INTERVAL 250000   // 1GB for blksize=4KB

static void do_HRC();
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
FILE *log_lat, *log_lat_pb;
char log_lat_path[] = "/home/fei/devel/logs/iolat.log";
char log_lat_pb_path[] = "/home/fei/devel/logs/lat_flushsmr.log";

void
trace_to_iocall(char *trace_file_path, int isWriteOnly,off_t startLBA)
{
    log_lat = fopen(log_lat_path, "w+");
    log_lat_pb = fopen(log_lat_pb_path, "w+");

    if(log_lat == NULL || log_lat_pb == NULL)
        usr_error("log file open failure.");

    if(I_AM_HRC_PROC)
    {
        do_HRC(startLBA);
        exit(EXIT_SUCCESS);
    }

    char		action;
    off_t		offset;
    char       *ssd_buffer;
    int	        returnCode;
    int         isFullSSDcache = 0;
    char        pipebuf[128];
    static struct timeval	tv_start_io, tv_stop_io;
    static char log[256];
    double io_latency;	// latency of each IO

#ifdef CG_THROTTLE
    static char* cgbuf;
    int returncode = posix_memalign(&cgbuf, 512, 4096);
#endif // CG_THROTTLE

    returnCode = posix_memalign(&ssd_buffer, 1024, 16*sizeof(char) * BLKSZ);
    if (returnCode < 0)
    {
        usr_warning("posix memalign error\n");
        //free(ssd_buffer);
        exit(-1);
    }
    int i;
    for (i = 0; i < 16 * BLKSZ; i++)
    {
        ssd_buffer[i] = '1';
    }

    _TimerLap(&tv_trace_start);
    static int req_cnt = 0;

    blkcnt_t total_n_req = isWriteOnly ? (blkcnt_t)REPORT_INTERVAL*500*10 : REPORT_INTERVAL*500*10;
    blkcnt_t skiprows = 0; //isWriteOnly ?  50000000 : 100000000;


    FILE *trace;
    if ((trace = fopen(trace_file_path, "rt")) == NULL)
    {
        usr_warning("Failed to open the trace file!\n");
        exit(EXIT_FAILURE);
    }

    int test_zone_num = 5000; // 20M * 5K = 100G data amount
    int inzone_wrt_blknum = UserId;
    
    int zone_cntr = 0;
    while (zone_cntr < test_zone_num) 
    {
        off_t zone_offset = (off_t)ZONESZ * zone_cntr;
        zone_cntr += 2;
        
        int inzone_cnt = 0;
        while (inzone_cnt < inzone_wrt_blknum)
        {
            off_t inzone_off;
            inzone_off = zone_offset + (inzone_wrt_blknum - inzone_cnt) * BLKSZ;
            inzone_cnt ++;

            _TimerLap(&tv_start_io);
            write_block(inzone_off, ssd_buffer);
            _TimerLap(&tv_stop_io);

            io_latency = TimerInterval_SECOND(&tv_start_io, &tv_stop_io);
#ifdef LOG_IO_LAT
            sprintf(log,"%f,%c\n", io_latency, action);
            _Log(log, log_lat);
#endif // LOG_IO_LAT
        }        

        //ResizeCacheUsage();
    }

    _TimerLap(&tv_trace_end);
    time_trace = Mirco2Sec(TimerInterval_MICRO(&tv_trace_start,&tv_trace_end));
    reportCurInfo();

    #ifdef HRC_PROCS_N
    for(i = 0; i < HRC_PROCS_N; i++)
    {
        sprintf(pipebuf,"EOF\n");
        pipe_write(PipeEnds_of_MAIN[i],pipebuf,64);
    }
    #endif // HRC_PROCS_N
    free(ssd_buffer);
    fclose(trace);
    fclose(log_lat);
    fclose(log_lat_pb);
}

static void
do_HRC()
{
#ifdef HRC_PROCS_N
    char	action;
    off_t   offset;
    int     i;
    int strlen = 64;
    char buf[128];
    int  ret;
    while((ret = read(PipeEnd_of_HRC, buf, strlen)) == strlen)
    {
        if(sscanf(buf,"%c,%lu\n", &action, &offset) < 0)
        {
            perror("HRC: sscanf string");
            exit(EXIT_FAILURE);
        }

        if (action == ACT_WRITE) // Write = 1
        {
            STT->reqcnt_w ++;
            STT->reqcnt_s ++;
            write_block(offset, NULL);
        }
        else if (action == ACT_READ)    // read = 9
        {
            STT->reqcnt_r ++;
            STT->reqcnt_s ++;
            read_block(offset,NULL);
        }

        if(STT->reqcnt_s % 10000== 0)
        {
            hrc_report();
        }
    }

    exit(EXIT_SUCCESS);
#endif
}

static void reportCurInfo()
{
    printf(" totalreqNum:%lu\n read_req_count: %lu\n write_req_count: %lu\n",
           STT->reqcnt_s,STT->reqcnt_r,STT->reqcnt_w);

    printf(" hit num:%lu\n hitnum_r:%lu\n hitnum_w:%lu\n",
           STT->hitnum_s,STT->hitnum_r,STT->hitnum_w);

    printf(" read_ssd_blocks:%lu\n flush_ssd_blocks:%lu\n read_hdd_blocks:%lu\n flush_dirty_blocks:%lu\n flush_clean_blocks:%lu\n",
           STT->load_ssd_blocks, STT->flush_ssd_blocks, STT->load_hdd_blocks, STT->flush_hdd_blocks, STT->flush_clean_blocks);

//    printf(" hash_miss:%lu\n hashmiss_read:%lu\n hashmiss_write:%lu\n",
//           STT->hashmiss_sum, STT->hashmiss_read, STT->hashmiss_write);

    printf(" total run time (s): %lf\n time_read_ssd : %lf\n time_write_ssd : %lf\n time_read_smr : %lf\n time_write_smr : %lf\n",
           time_trace, STT->time_read_ssd, STT->time_write_ssd, STT->time_read_hdd, STT->time_write_hdd);
    printf(" Batch flush HDD time:%lu\n",msec_bw_hdd);

    printf(" Cache Proportion(R/W): [%ld/%ld]\n", STT->incache_n_clean,STT->incache_n_dirty);
    printf(" wt_hit_rd: %lu\n rd_hit_wt: %lu\n",STT->wt_hit_rd, STT->rd_hit_wt);
}

static void report_ontime()
{
//    _TimerLap(&tv_checkpoint);
//    double timecost = Mirco2Sec(TimerInterval_SECOND(&tv_trace_start,&tv_checkpoint));

//     printf("totalreq:%lu, readreq:%lu, hit:%lu, readhit:%lu, flush_ssd_blk:%lu flush_hdd_blk:%lu, hashmiss:%lu, readhassmiss:%lu writehassmiss:%lu\n",
//           STT->reqcnt_s,STT->reqcnt_r, STT->hitnum_s, STT->hitnum_r, STT->flush_ssd_blocks, STT->flush_hdd_blocks, STT->hashmiss_sum, STT->hashmiss_read, STT->hashmiss_write);
        printf("totalreq:%lu, readreq:%lu, wrtreq:%lu, hit:%lu, readhit:%lu, flush_ssd_blk:%lu flush_dirty_blk:%lu\n",
           STT->reqcnt_s, STT->reqcnt_r, STT->reqcnt_w, STT->hitnum_s, STT->hitnum_r, STT->flush_ssd_blocks, STT->flush_hdd_blocks);
        _TimerLap(&tv_trace_end);
        int timecost = Mirco2Sec(TimerInterval_MICRO(&tv_trace_start,&tv_trace_end));
        printf("current run time: %d\n",timecost);
}

static void resetStatics()
{

//    STT->hitnum_s = 0;
//    STT->hitnum_r = 0;
//    STT->hitnum_w = 0;
    STT->load_ssd_blocks = 0;
    STT->flush_ssd_blocks = 0;

    STT->time_read_hdd = 0.0;
    STT->time_write_hdd = 0.0;
    STT->time_read_ssd = 0.0;
    STT->time_write_ssd = 0.0;
    STT->hashmiss_sum = 0;
    STT->hashmiss_read = 0;
    STT->hashmiss_write = 0;
    msec_bw_hdd = 0;
}

