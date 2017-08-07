#include <stdio.h>
#include <stdlib.h>

#include "smr-simulator.h"
#include "cache.h"

static bool isSamessd(DespTag *, DespTag *);

void initSSDTable(size_t size)
{
	ssd_hashtable = (SSDHashBucket*)malloc(sizeof(SSDHashBucket)*size);
	size_t i;
	SSDHashBucket *ssd_hash = ssd_hashtable;
	for (i = 0; i < size; ssd_hash++, i++){
		ssd_hash->despId = -1;
		ssd_hash->hash_key.offset = -1;
		ssd_hash->next_item = NULL;
	}
}

unsigned long ssdtableHashcode(DespTag *tag)
{
	unsigned long ssd_hash = (tag->offset / SSD_BUFFER_SIZE) % NBLOCK_SMR_FIFO;
	return ssd_hash;
}

long ssdtableLookup(DespTag *tag, unsigned long hash_code)
{
	if (DEBUG)
		printf("[INFO] Lookup tag: %lu\n",tag->offset);
	SSDHashBucket *nowbucket = GetSSDHashBucket(hash_code);
	while (nowbucket != NULL) {
	//	printf("nowbucket->buf_id = %u %u %u\n", nowbucket->hash_key.rel.database, nowbucket->hash_key.rel.relation, nowbucket->hash_key.block_num);
		if (isSamessd(&nowbucket->hash_key, tag)) {
	//		printf("find\n");
			return nowbucket->despId;
		}
		nowbucket = nowbucket->next_item;
	}
//	printf("no find\n");

	return -1;
}

long ssdtableInsert(DespTag *tag, unsigned long hash_code, long despId)
{
	if (DEBUG)
		printf("[INFO] Insert tag: %lu, hash_code=%lu\n",tag->offset, hash_code);
	SSDHashBucket *nowbucket = GetSSDHashBucket(hash_code);
	while (nowbucket->next_item != NULL && nowbucket != NULL) {
		if (isSamessd(&nowbucket->hash_key, tag)) {
			return nowbucket->despId;
		}
		nowbucket = nowbucket->next_item;
	}
	if (nowbucket != NULL) {
		SSDHashBucket *newitem = (SSDHashBucket*)malloc(sizeof(SSDHashBucket));
		newitem->hash_key = *tag;
		newitem->despId = despId;
		newitem->next_item = NULL;
		nowbucket->next_item = newitem;
	}
	else {
		nowbucket->hash_key = *tag;
		nowbucket->despId = despId;
		nowbucket->next_item = NULL;
	}

	return -1;
}

long ssdtableDelete(DespTag *tag, unsigned long hash_code)
{
	if (DEBUG)
		printf("[INFO] Delete tag: %lu, hash_code=%lu\n",tag->offset, hash_code);
	SSDHashBucket *nowbucket = GetSSDHashBucket(hash_code);
	long del_id;
	SSDHashBucket *delitem;
	nowbucket->next_item;
	while (nowbucket->next_item != NULL && nowbucket != NULL) {
		if (isSamessd(&nowbucket->next_item->hash_key, tag)) {
			del_id = nowbucket->next_item->despId;
			break;
		}
		nowbucket = nowbucket->next_item;
	}
	//printf("not found2\n");
	if (isSamessd(&nowbucket->hash_key, tag)) {
		del_id = nowbucket->despId;
	}
	//printf("not found3\n");
	if (nowbucket->next_item != NULL) {
		delitem = nowbucket->next_item;
		nowbucket->next_item = nowbucket->next_item->next_item;
		free(delitem);
		return del_id;
	}
	else {
		delitem = nowbucket->next_item;
		nowbucket->next_item = NULL;
		free(delitem);
		return del_id;
	}

	return -1;
}

static bool isSamessd(DespTag *tag1, DespTag *tag2)
{
	if (tag1->offset != tag2->offset)
		return 0;
	else return 1;
}
