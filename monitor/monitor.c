#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>             /* getopt */
#include "global.h"
#include "shmlib.h"

const char* PATH_SHM = "/dev/shm";

static char cmdbuf[1024];
static void ReadCommond();
static int readANS(char* buf,int cnt);

char* str_help = "Command action\n\ta\tCheck processes runtime state.\n\tp\tRe-Size the Cache Usage upper limitation.\n";

int
main(int argc, char** argv)
{

    TimerLog();

    while(1)
    {
        ReadCommond();
    }
}

void TimerLog()
{
    struct RuntimeSTAT *userstt1, *userstt2, *userstt3;
    userstt1 = (struct RuntimeSTAT*)SHM_get("STAT_b3_u1_t0",sizeof(struct RuntimeSTAT));
    userstt2 = (struct RuntimeSTAT*)SHM_get("STAT_b3_u2_t0",sizeof(struct RuntimeSTAT));
    userstt3 = (struct RuntimeSTAT*)SHM_get("STAT_b3_u3_t0",sizeof(struct RuntimeSTAT));

    FILE *log1,*log2,*log3;
    log1 = fopen("/home/outputs/runtimelog1","at");
    log2 = fopen("/home/outputs/runtimelog2","at");
    log3 = fopen("/home/outputs/runtimelog3","at");
    unsigned long id = 0;
    while(1)
    {
        id++;
        printtStatCSV(userstt1,log1,id);
        printtStatCSV(userstt2,log2,id);
        printtStatCSV(userstt3,log3,id);
        sleep(60);
    }
}
void CheckRuntime()
{
    struct RuntimeSTAT* usrStat;
    char* str_selectbatch = "Batch ID: ";
    char* str_selectuser = "User ID: ";
    char* str_nouser = "There is no the user\n";
    char* str_pipeerr = "something error...\n";

    char str_buf[1024];

    char str_bid[10];
    char str_uid[10];
    int batchId;
    int usrId;
    FILE* fd_shell;
    char command[1024];
    char shellbuf[1024];

    while(1)
    {
        Ask(str_selectbatch);
        if(readANS(str_bid,9)<0) return;
        batchId = atoi(str_bid);
        sprintf(command,"ls -l %s/STAT_b%d_* | grep \"^-\"|wc -l",PATH_SHM,batchId);

        fd_shell = popen(command,"r");
        if(fd_shell <=0 )
        {
            Say(str_pipeerr);
            continue;
        }
        fgets(shellbuf,1023,fd_shell);
        pclose(fd_shell);

        int usercnt = atoi(shellbuf);
        sprintf(str_buf,"%d Users in this batch.\n",usercnt);
        Say(str_buf);
        if(usercnt==0) continue;

        do
        {
            Ask(str_selectuser);
            if(readANS(str_uid,9)<0) return;
        }
        while(str_uid[0] == '\n');
        usrId = atoi(str_uid);
        sprintf(command,"ls %s/STAT_b%d_u%d*",PATH_SHM,batchId,usrId);

        fd_shell = popen(command,"r");
        if(fd_shell <= 0)
        {
            Say(str_pipeerr);
            continue;
        }
        fgets(shellbuf,1023,fd_shell);

        int len = strlen(shellbuf);
        if(len <= 0)
        {
            Say(str_nouser);
            continue;
        }
        shellbuf[len-1]=0;
        char* fname = strrchr(shellbuf,'/');
        fname++;

        usrStat = (struct RuntimeSTAT*)SHM_get(fname,sizeof(struct RuntimeSTAT));
        if(usrStat == NULL)
        {
            Say(str_pipeerr);
            continue;
        }

        sprintf(str_buf,"User #%d Runtime Statistic is here:\n",usrId);
        Say(str_buf);
        PrintStatInfo(usrStat);
    }
}

void printtStatCSV(struct RuntimeSTAT* STT,FILE* file,unsigned long id)
{
    fprintf(file,"%lu\t",id);
    int hitRate = (double)STT->hitnum_s/STT->reqcnt_s * 100;
    fprintf(file,"%d\t",hitRate);
    fprintf(file,"%lu\t", STT->cacheLimit);
    fprintf(file,"%lu\t", STT->cacheUsage);

    fprintf(file,"%lu\t", STT->reqcnt_s);
    fprintf(file,"%lu\t", STT->reqcnt_r);
    fprintf(file,"%lu\t", STT->reqcnt_w);

    fprintf(file,"%lu\t", STT->hitnum_s);
    fprintf(file,"%lu\t", STT->hitnum_r);
    fprintf(file,"%lu\t", STT->hitnum_w);

    fprintf(file,"%lu\t", STT->load_ssd_blocks);
    fprintf(file,"%lu\t", STT->load_hdd_blocks);
    fprintf(file,"%lu\t", STT->flush_hdd_blocks);
    fprintf(file,"%lu\t", STT->flush_ssd_blocks);
    fprintf(file,"%lu\t", STT->flush_clean_blocks);

    fprintf(file,"%lf\t", STT->time_read_ssd);
    fprintf(file,"%lf\t", STT->time_read_hdd);
    fprintf(file,"%lf\t", STT->time_write_ssd);
    fprintf(file,"%lf\t", STT->time_write_hdd);

    fprintf(file,"%lu\t", STT->hashmiss_sum);
    fprintf(file,"%lu\t", STT->hashmiss_read);
    fprintf(file,"%lu\n", STT->hashmiss_write);
    fflush(file);
}

void PrintStatInfo(struct RuntimeSTAT* STT,FILE* file)
{
    fprintf(file,"**********************************************\n");
    fprintf(file,"BID=%d\tUID=%d\tTID=%d\t\n",STT->batchId,STT->userId,STT->traceId);
    fprintf(file,"IO Mode:%s\n",STT->isWriteOnly?"WO":"RW");
    fprintf(file,"Start LBA:%lu\n",STT->startLBA);
    fprintf(file,"-------------------------------------\n");

    int hitRate = (double)STT->hitnum_s/STT->reqcnt_s * 100;
    fprintf(file,"Hit Rate:\t%d%\n",hitRate);
    fprintf(file,"Max CacheBlk:\t%lu\n", STT->cacheLimit);
    fprintf(file,"Cur CacheBkl:\t%lu\n", STT->cacheUsage);

    fprintf(file,"Req Count:\t%lu\n", STT->reqcnt_s);
    fprintf(file,"Req Read:\t%lu\n", STT->reqcnt_r);
    fprintf(file,"Req Write:\t%lu\n", STT->reqcnt_w);

    fprintf(file,"Hit Sum:\t%lu\n", STT->hitnum_s);
    fprintf(file,"Hit Read:\t%lu\n", STT->hitnum_r);
    fprintf(file,"Hit Write:\t%lu\n", STT->hitnum_w);

    fprintf(file,"read ssd blk:\t%lu\n", STT->load_ssd_blocks);
    fprintf(file,"read hdd blk:\t%lu\n", STT->load_hdd_blocks);
    fprintf(file,"write hdd bl:\t%lu\n", STT->flush_hdd_blocks);
    fprintf(file,"write ssd bkl:\t%lu\n", STT->flush_ssd_blocks);
    fprintf(file,"drop clean blk:\t%lu\n", STT->flush_clean_blocks);

    fprintf(file,"time read ssd:\t%lf\n", STT->time_read_ssd);
    fprintf(file,"time read hdd:\t%lf\n", STT->time_read_hdd);
    fprintf(file,"time write ssd:\t%lf\n", STT->time_write_ssd);
    fprintf(file,"time write hdd:\t%lf\n", STT->time_write_hdd);

    fprintf(file,"HashMiss:\t%lu\n", STT->hashmiss_sum);
    fprintf(file,"HashMiss Read:\t%lu\n", STT->hashmiss_read);
    fprintf(file,"HashMiss Write:\t%lu\n", STT->hashmiss_write);
    fprintf(file,"**********************************************\n\n");
    fflush(file);
}

void ResizeCache()
{
    struct RuntimeSTAT* usrStat;
    char* str_selectbatch = "Batch ID: ";
    char* str_selectuser = "User ID: ";
    char* str_nouser = "There is no the user\n";
    char* str_pipeerr = "something error...\n";

    char str_buf[1024];

    char str_bid[10];
    char str_uid[10];
    int batchId;
    int usrId;
    FILE* fd_shell;
    char command[1024];
    char shellbuf[1024];

    while(1)
    {
        Ask(str_selectbatch);
        if(readANS(str_bid,9)<0) return;
        batchId = atoi(str_bid);
        sprintf(command,"ls -l %s/STAT_b%d_* | grep \"^-\"|wc -l",PATH_SHM,batchId);

        fd_shell = popen(command,"r");
        if(fd_shell <=0 )
        {
            Say(str_pipeerr);
            continue;
        }
        fgets(shellbuf,1023,fd_shell);
        pclose(fd_shell);

        int usercnt = atoi(shellbuf);
        sprintf(str_buf,"%d Users in this batch.\n",usercnt);
        Say(str_buf);
        if(usercnt==0) continue;

        do
        {
            Ask(str_selectuser);
            if(readANS(str_uid,9)<0) return;
        }
        while(str_uid[0] == '\n');
        usrId = atoi(str_uid);
        sprintf(command,"ls %s/STAT_b%d_u%d*",PATH_SHM,batchId,usrId);

        fd_shell = popen(command,"r");
        if(fd_shell <= 0)
        {
            Say(str_pipeerr);
            continue;
        }
        fgets(shellbuf,1023,fd_shell);

        int len = strlen(shellbuf);
        if(len <= 0)
        {
            Say(str_nouser);
            continue;
        }
        shellbuf[len-1]=0;
        char* fname = strrchr(shellbuf,'/');
        fname++;

        usrStat = (struct RuntimeSTAT*)SHM_get(fname,sizeof(struct RuntimeSTAT));
        if(usrStat == NULL)
        {
            Say(str_pipeerr);
            continue;
        }

        sprintf(str_buf,"User #%d Runtime Statistic is here:\n",usrId);
        Say(str_buf);
        PrintStatInfo(usrStat,stdin);

        Ask("Re-Size the cahe limit to:");
        if(readANS(str_buf,64)<0) return;
        blksize_t cachelimit = atol(str_buf);
        usrStat->cacheLimit = cachelimit;
    }
}

void Ask(char* msg)
{
    printf("[monitor]# %s",msg);
}

void Say(char* msg)
{
    printf(msg);
}

static
int readANS(char* buf,int cnt)
{
    fgets(buf,cnt,stdin);
    if(buf[0] == 'q')
        return -1;
    return 0;
}

static
void ReadCommond()
{
    Ask("Command (h for help): ");
    fgets(cmdbuf,10,stdin);
    int cmd = cmdbuf[0];
    switch(cmd)
    {
    case 'a':
        CheckRuntime();
        break;
    case 'p':
        ResizeCache();
        break;
    case 'h':
        Say(str_help);
    }
}
