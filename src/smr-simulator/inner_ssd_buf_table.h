#ifndef SSDBUFTABLE_H
#define SSDBUFTABLE_H

extern void initSSDTable(int size);
extern unsigned ssdtableHashcode(DespTag *tag);
extern int ssdtableLookup(DespTag *tag, unsigned hash_code);
extern int ssdtableInsert(DespTag *tag, unsigned hash_code, int despId);
extern int ssdtableDelete(DespTag *tag, unsigned hash_code);
#endif   /* SSDBUFTABLE_H */
