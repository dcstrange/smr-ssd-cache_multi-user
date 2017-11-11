#define DEBUG 0
#ifndef _PORE_H
#define _PORE_H
#include "cache.h"
#include "global.h"

typedef struct StrategyDesp_pore
{
    long            serial_id;
    SSDBufTag       ssd_buf_tag;
    unsigned 	    flag;
    long            pre,next;
    unsigned long   heat;
    long  	    stamp;
}StrategyDesp_pore;

//typedef struct StrategyCtrl_pore
//{
//    long            freeId;     // Head of list of free ssds
//    long            n_used;
//}StrategyCtrl_pore;

typedef struct ZoneCtrl
{
    unsigned long   zoneId;
    long            heat;
    long            pagecnt_dirty;
    long            pagecnt_clean;
    long            weight;
    long            head,tail;
    long	    stamp;
}ZoneCtrl;

extern int InitPORE();
extern int LogInPoreBuffer(long despId, SSDBufTag tag, unsigned flag);
extern int HitPoreBuffer(long despId, unsigned flag);
extern long LogOutDesp_pore();

#endif // _PORE_H
