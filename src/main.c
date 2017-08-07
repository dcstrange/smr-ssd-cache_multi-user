/*
 * main.c
 */
#define _GNU_SOURCE 1
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <getopt.h>
#include <unistd.h>

#include "report.h"
#include "shmlib.h"
#include "global.h"
#include "cache.h"
//#include "smr-simulator/smr-simulator.h"
#include "trace2call.h"

extern void FunctionalTest();

unsigned int INIT_PROCESS = 0;
void ramdisk_iotest()
{
    int fdram = open("/mnt/ramdisk/ramdisk",O_RDWR | O_DIRECT);
    printf("fdram=%d\n",fdram);

    char* buf;
    posix_memalign(&buf, 512, 4096);
    size_t count = 512;
    off_t offset = 0;

    int r;
    while(1)
    {
        r = pwrite(fdram,buf,count,offset);
        if(r<=0)
        {
            printf("write ramdisk error:%d\n",r);
            exit(1);
        }
    }
}

char* tracefile[] = {"/home/trace/src1_2.csv.req",
                     "/home/trace/wdev_0.csv.req",
                     "/home/trace/hm_0.csv.req",
                     "/home/trace/mds_0.csv.req",
                     "/home/trace/prn_0.csv.req",
                     "/home/trace/rsrch_0.csv.req",
                     "/home/trace/stg_0.csv.req",
                     "/home/trace/ts_0.csv.req",
                     "/home/trace/usr_0.csv.req",
                     "/home/trace/web_0.csv.req"
                    };



int
main(int argc, char** argv)
{
    //FunctionalTest();
    //ramdisk_iotest();

// 1 1 1 0 0 100000 100000
// 1 1 0 0 0 100000 100000
    if(argc == 8)
    {
        BatchId = atoi(argv[1]);
        UserId = atoi(argv[2]);
        TraceId = atoi(argv[3]);
        WriteOnly = atoi(argv[4]);
        StartLBA = atol(argv[5]);
        NBLOCK_SSD_CACHE = NTABLE_SSD_CACHE = atol(argv[6]);
        NBLOCK_SMR_FIFO = atol(argv[7]);
        EvictStrategy = PORE;
        //EvictStrategy = LRU_private;
    }
    else
    {
        printf("parameters are wrong %d\n", argc);
        exit(-1);
    }

//        NBLOCK_SSD_CACHE = NTABLE_SSD_CACHE = 50000;
//        isWriteOnly = 0;
//        traceId = 1;
//        startLBA = 0;
    //NBLOCK_SSD_CACHE = NTABLE_SSD_CACHE = 500000;//280M //50000; // 200MB

#ifdef CG_THROTTLE
    init_cgdev();
#endif // CG_THROTTLE

    initLog();
    initRuntimeInfo();
    initSSD();



    hdd_fd = open(smr_device, O_RDWR | O_DIRECT);
    ssd_fd = open(ssd_device, O_RDWR | O_DIRECT);

    printf("Device ID: hdd=%d, ssd=%d\n",hdd_fd,ssd_fd);

#ifdef SIMULATION
    initFIFOCache();
#endif

    trace_to_iocall(tracefile[TraceId],WriteOnly,StartLBA);

#ifdef SIMULATION
    PrintSimulatorStatistic();
#endif
    close(hdd_fd);
    close(ssd_fd);
    CloseLogFile();

    return 0;
}

int initRuntimeInfo()
{
    char str_STT[50];
    sprintf(str_STT,"STAT_b%d_u%d_t%d",BatchId,UserId,TraceId);
    if((STT = (struct RuntimeSTAT*)SHM_alloc(str_STT,sizeof(struct RuntimeSTAT))) == NULL)
        return errno;

    STT->batchId = BatchId;
    STT->userId = UserId;
    STT->traceId = TraceId;
    STT->startLBA = StartLBA;
    STT->isWriteOnly = WriteOnly;
    STT->cacheUsage = 0;
    STT->cacheLimit = 0x7fffffffffffffff;
    return 0;
}

int init_cgdev()
{
    sprintf(ram_device,"/mnt/ramdisk/ramdisk_%d",STT->userId);
    ram_fd = open(ram_device,O_RDWR | O_DIRECT);
    printf("fdram=%d\n",ram_fd);
}


int initLog()
{
    char logpath[50];
    sprintf(logpath,"%s/b%d_u%d_t%d_bs%d.log",PATH_LOG,BatchId,UserId,TraceId,BatchSize);
    int rt = 0;
    if((rt = OpenLogFile(logpath)) < 0)
    {
        error("open log file failure.\n");

    }
    return rt;
}
