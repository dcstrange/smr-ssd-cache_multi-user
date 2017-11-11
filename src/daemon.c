#include <stdio.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include "global.h"
#include "daemon.h"

extern struct RuntimeSTAT* STT;

void* daemon_proc()
{
    char path[] = "/tmp/daemon_cachesys.out";
    unlink(path);
    FILE* file = fopen(path, "at+"); // append mode
    char str_runtime[4096];

    pthread_t th = pthread_self();
    pthread_detach(th);

    time_t rawtime;
    struct tm * timeinfo;

    struct RuntimeSTAT lastSTT, curSTT;
    lastSTT = *STT;

    /* Bandwidth */
    int bw_s, bw_r, bw_w; // KB/s
    int bw_smr_r, bw_smr_w;

    while(1)
    {
        sleep(1);
        /* Load runtime arverage IO speed(KB/s) */
        curSTT = *STT;

        bw_s = (curSTT.reqcnt_s - lastSTT.reqcnt_s) * 4;
        bw_r = (curSTT.reqcnt_r - lastSTT.reqcnt_r) * 4;
        bw_w = (curSTT.reqcnt_w - lastSTT.reqcnt_w) * 4;

        bw_smr_r = (curSTT.load_hdd_blocks - lastSTT.load_hdd_blocks) *4;
        bw_smr_w = (curSTT.flush_hdd_blocks - lastSTT.flush_hdd_blocks) * 4;

        lastSTT = curSTT;

        /* Current WrtAmp */
        double wrtamp = STT->wtrAmp_cur;
        /* Process Percentage */
        double progress = (double)STT->reqcnt_s / STT->trace_req_amount * 100;

        /* Output */
        time(&rawtime);
        timeinfo = localtime (&rawtime);

        sprintf(str_runtime, "%sReqcnt(s,r,w):\t%ld\t%ld\t%ld\nbandwidth(s,r,w):\t%d,\t%d,\t%d\n",asctime(timeinfo),curSTT.reqcnt_s,curSTT.reqcnt_r,curSTT.reqcnt_w,bw_s,bw_r,bw_w);
        fwrite(str_runtime,strlen(str_runtime),1,file);
        sprintf(str_runtime, "\tSMR(r,w):\t\t%d,\t%d\n", bw_smr_r,bw_smr_w);
        fwrite(str_runtime,strlen(str_runtime),1,file);
        sprintf(str_runtime, "\twrtAmp:%d\tprogress:%d\%\n",(int)wrtamp,(int)progress);
        fwrite(str_runtime,strlen(str_runtime),1,file);
        fflush(file);
    }
}

