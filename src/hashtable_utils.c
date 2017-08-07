#include <stdio.h>
#include <stdlib.h>
#include "shmlib.h"
#include "cache.h"
#include "hashtable_utils.h"
#include "global.h"

extern void _LOCK(pthread_mutex_t* lock);
extern void _UNLOCK(pthread_mutex_t* lock);

SSDBufHashBucket*   ssd_buf_hashtable;

int HashTab_Init()
{
    ssd_buf_hashtable = (SSDBufHashBucket*)malloc(sizeof(SSDBufHashBucket)*NTABLE_SSD_CACHE);
    if(ssd_buf_hashtable == NULL)
        return -1;

    SSDBufHashBucket* bucket = ssd_buf_hashtable;
    size_t i = 0;
    for(i = 0; i < NBLOCK_SSD_CACHE; bucket++, i++)
    {
        bucket->desp_serial_id = -1;
        bucket->hash_key.offset = -1;
        bucket->next_item = NULL;
    }
    return 0;
//    int stat = SHM_lock_n_check("LOCK_SSDBUF_HASHTABLE");
//    if(stat == 0)
//    {
//        ssd_buf_hash_ctrl = (SSDBufHashCtrl*)SHM_alloc(SHM_SSDBUF_HASHTABLE_CTRL,sizeof(SSDBufHashCtrl));
//        ssd_buf_hashtable = (SSDBufHashBucket*)SHM_alloc(SHM_SSDBUF_HASHTABLE,sizeof(SSDBufHashBucket)*size);
//        ssd_buf_hashdesps = (SSDBufHashBucket*)SHM_alloc(SHM_SSDBUF_HASHDESPS,sizeof(SSDBufHashBucket)*NBLOCK_SSD_CACHE);
//
//        SHM_mutex_init(&ssd_buf_hash_ctrl->lock);
//
//        size_t i;
//        SSDBufHashBucket *ssd_buf_hash = ssd_buf_hashtable;
//        for (i = 0; i < size; ssd_buf_hash++, i++)
//        {
//            ssd_buf_hash->ssd_buf_id = -1;
//            ssd_buf_hash->hash_key.offset = -1;
//            ssd_buf_hash->next_item = NULL;
//        }
//        SSDBufHashBucket *hash_desp = ssd_buf_hashdesps;
//        for(i = 0; i <NBLOCK_SSD_CACHE; i++)
//        {
//            ssd_buf_hash->ssd_buf_id = i;
//            ssd_buf_hash->hash_key.offset = -1;
//            ssd_buf_hash->next_item = NULL;
//        }
//    }
//    else
//    {
//        ssd_buf_hash_ctrl = (SSDBufHashCtrl*)SHM_get(SHM_SSDBUF_HASHTABLE_CTRL,sizeof(SSDBufHashCtrl));
//        ssd_buf_hashtable = (SSDBufHashBucket *)SHM_get(SHM_SSDBUF_HASHTABLE,sizeof(SSDBufHashBucket)*size);
//        ssd_buf_hashdesps = (SSDBufHashBucket*)SHM_get(SHM_SSDBUF_HASHDESPS,sizeof(SSDBufHashBucket)*NBLOCK_SSD_CACHE);
//
//    }
//    SHM_unlock("LOCK_SSDBUF_HASHTABLE");
//    return stat;
}

unsigned long HashTab_GetHashCode(SSDBufTag *ssd_buf_tag)
{
    unsigned long hashcode = (ssd_buf_tag->offset / SSD_BUFFER_SIZE) % NTABLE_SSD_CACHE;
    return hashcode;
}

long HashTab_Lookup(SSDBufTag *ssd_buf_tag, unsigned long hash_code)
{
    if (DEBUG)
        printf("[INFO] Lookup ssd_buf_tag: %lu\n",ssd_buf_tag->offset);
    SSDBufHashBucket *nowbucket = GetSSDBufHashBucket(hash_code);
    while (nowbucket != NULL)
    {
        if (isSamebuf(&nowbucket->hash_key, ssd_buf_tag))
        {
            return nowbucket->desp_serial_id;
        }
        nowbucket = nowbucket->next_item;
    }

    return -1;
}

long HashTab_Insert(SSDBufTag *ssd_buf_tag, unsigned long hash_code, long desp_serial_id)
{
    if (DEBUG)
        printf("[INFO] Insert buf_tag: %lu\n",ssd_buf_tag->offset);

    SSDBufHashBucket *nowbucket = GetSSDBufHashBucket(hash_code);
    if(nowbucket == NULL)
    {
        printf("[ERROR] Insert HashBucket: Cannot get HashBucket.\n");
        exit(1);
    }
    while (nowbucket->next_item != NULL)
    {
        nowbucket = nowbucket->next_item;
    }

    SSDBufHashBucket *newitem = (SSDBufHashBucket*)malloc(sizeof(SSDBufHashBucket));
    newitem->hash_key = *ssd_buf_tag;
    newitem->desp_serial_id = desp_serial_id;
    newitem->next_item = NULL;

    nowbucket->next_item = newitem;
    return 0;
}

long HashTab_Delete(SSDBufTag *ssd_buf_tag, unsigned long hash_code)
{
    if (DEBUG)
        printf("[INFO] Delete buf_tag: %lu\n",ssd_buf_tag->offset);

    long del_id;
    SSDBufHashBucket *delitem;
    SSDBufHashBucket *nowbucket = GetSSDBufHashBucket(hash_code);

    while (nowbucket->next_item != NULL)
    {
        if (isSamebuf(&nowbucket->next_item->hash_key, ssd_buf_tag))
        {
            delitem = nowbucket->next_item;
            del_id = nowbucket->next_item->desp_serial_id;
            nowbucket->next_item = delitem->next_item;
            free(delitem);
            return del_id;
        }
        nowbucket = nowbucket->next_item;
    }
    return -1;
}


