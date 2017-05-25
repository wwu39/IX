
#include "ix.h"
#include <sys/stat.h>
#include <cstring>
#include <iostream>

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
    // If the file already exists, error
    if (fileExists(fileName))
        return IX_FILE_EXISTS;

    // Attempt to open the file for writing
    FILE *pFile = fopen(fileName.c_str(), "wb");
    // Return an error if we fail
    if (pFile == NULL)
        return IX_OPEN_FAILED;
    
    fclose (pFile);
    return SUCCESS;
}

RC IndexManager::destroyFile(const string &fileName)
{
    if (remove(fileName.c_str()) != 0)
        return IX_REMOVE_FAILED;
    return SUCCESS;
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
    // If this handle already has an open file, error
    if (ixfileHandle.getfd() != NULL)
        return IX_HANDLE_IN_USE;

    // If the file doesn't exist, error
    if (!fileExists(fileName.c_str()))
        return PFM_FILE_DN_EXIST;
    
    // Open the file for reading/writing in binary mode
    FILE *pFile;
    pFile = fopen(fileName.c_str(), "rb+");
    // If we fail, error
    if (pFile == NULL) return IX_OPEN_FAILED;

    ixfileHandle.setfd(pFile);

    return SUCCESS;

}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    FILE *pFile = ixfileHandle.getfd();
    if (pFile == NULL) return 1;
    fclose(pFile);
    ixfileHandle.setfd(NULL);
    return SUCCESS;
}

//***********************************************************************************************************************************
//                                                                INSERT                                                           **
//***********************************************************************************************************************************
RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    // check the attribute
    int numOfPage = ixfileHandle.getNumberOfPages();
    if (numOfPage == 0) { // first insert
        // initialize the file
        initIXfile(attribute, ixfileHandle);
    } else {
        // check attribute
        if (!checkIXAttribute(attribute, ixfileHandle)) return IX_ATTR_MISMATCH;
    }
    void * page = malloc(PAGE_SIZE);
    // find the leaf page it belongs to
    int targetPageNum = findPosition(ixfileHandle, attribute, key, page);
    int FSSzie = getPageFreeSpaceSize(page);
    int attrSize = getAttrSize(attribute, key);
    if (attrSize + (int)sizeof(RID) + (int)sizeof(Entry) <= FSSzie) { // can be fit in
        insertEntryToPage(attribute, key, rid, page);
        ixfileHandle.writePage(targetPageNum, page);
    } else {
        // split to a new page
        void * newPage = malloc(PAGE_SIZE);
        void * pivot = malloc(PAGE_SIZE);

        // new page with page number numOfPage
        int newPageNum = numOfPage;
        ixfileHandle.appendPage(newPage);

        // split page into two pages: page and newPage
        // return the pivot
        // pivot should be inserted to the parent
        splitPages(page, newPage, attribute, key, rid, pivot);

        
        // update headers
        IX_SlotDirectoryHeader oldHeader = getPageHeader(page);
        IX_SlotDirectoryHeader newHeader = getPageHeader(newPage);
        newHeader.next = oldHeader.next;
        oldHeader.next = newPageNum;
        setPageHeader(page, oldHeader);
        setPageHeader(newPage, newHeader);
        ixfileHandle.writePage(targetPageNum, page);
        ixfileHandle.writePage(newPageNum, newPage);
        free(newPage);
        // recursively split ancestors, create new pages if necesasary
        // return the parent of new page
        splitAncestors(targetPageNum, attribute, pivot, newPageNum, ixfileHandle, oldHeader.parent);

    }
    free(page);
    return SUCCESS;
}

void IndexManager::splitAncestors(int leftChild, const Attribute &attribute, const void * pivot, int pointer, IXFileHandle &ixfileHandle, const int parent)
{
    void * page = malloc(PAGE_SIZE);
    if (parent == 0) { // hit the root
        newNonLeafPage(leftChild, attribute, pivot, pointer, page); // insert P0 and K1P1
        int newPageNum = ixfileHandle.getNumberOfPages();
        ixfileHandle.appendPage(page); // flush new page
        // root's parent is 0
        setParent(ixfileHandle, newPageNum, 0);
        // set parents of the children
        setParent(ixfileHandle, leftChild, newPageNum);
        setParent(ixfileHandle, pointer, newPageNum);
        // this is the new root
        setRoot(ixfileHandle, newPageNum);
        free(page);
        return;
    }
    ixfileHandle.readPage(parent, page);
    int FSSzie = getPageFreeSpaceSize(page);
    int attrSize = getAttrSize(attribute, pivot);
    if (attrSize + (int)sizeof(int) + (int)sizeof(Entry) <= FSSzie) { // can be fit in
        insertEntryToPage(attribute, pivot, pointer, page); // put it there
        ixfileHandle.writePage(parent, page); // flush it and done
        // set parents of the children
        setParent(ixfileHandle, leftChild, parent);
        setParent(ixfileHandle, pointer, parent);
        free(page);
    } else {
        void * newPage = malloc(PAGE_SIZE);
        void * newPivot = malloc(PAGE_SIZE);

        // new page with page number numOfPage
        int newPageNum = ixfileHandle.getNumberOfPages();
        ixfileHandle.appendPage(newPage);
        
        // split page into two pages: page and newPage
        // return the pivot
        // pivot should be inserted to the parent
        splitPages(page, newPage, attribute, pivot, pointer, newPivot);
        IX_SlotDirectoryHeader oldHeader = getPageHeader(page);
        ixfileHandle.writePage(parent, page);
        ixfileHandle.writePage(newPageNum, newPage);
        setParent(ixfileHandle, leftChild, parent);
        if (keyCompare(attribute, newPivot, pivot) <= 0) 
            setParent(ixfileHandle, pointer, newPageNum);
        else setParent(ixfileHandle, pointer, parent);
        free(newPage);
        free(page);
        splitAncestors(parent, attribute, newPivot, newPageNum, ixfileHandle, oldHeader.parent);
        free(newPivot);
    }
}

void IndexManager::newNonLeafPage(int left, const Attribute &attribute, const void * key, int right, void * page)
{
    // insert P0
    Entry entry;
    memcpy(page, &left, sizeof(int));
    entry.offset = 0;
    entry.length = sizeof(int);
    setEntry(0, entry, page);

    // insert P1K1
    int attrSize = getAttrSize(attribute, key);
    memcpy((char *)page + sizeof(int), key, attrSize);
    memcpy((char *)page + sizeof(int) + attrSize, &right, sizeof(int));
    entry.offset = sizeof(int);
    entry.length = attrSize + sizeof(int);
    setEntry(1, entry, page);

    // insert header
    IX_SlotDirectoryHeader header;
    header.FS = entry.offset + entry.length;
    header.N = 2;
    header.leaf = 0;
    setPageHeader(page, header);
}

void IndexManager::setParent(IXFileHandle &ixfileHandle, int child, int parent)
{
    void * page = malloc(PAGE_SIZE);
    ixfileHandle.readPage(child, page);
    IX_SlotDirectoryHeader header = getPageHeader(page);
    header.parent = parent;
    setPageHeader(page, header);
    ixfileHandle.writePage(child, page);
    free(page);
}

void IndexManager::splitPages(void * oldPage, void* newPage, const Attribute &attribute, const void *key, const RID& rid, void * pivot)
{
    void * doublepage = malloc(2 * PAGE_SIZE);
    prepareDoublePage(oldPage, attribute, key, rid, doublepage);

    char * upper = (char *)doublepage;
    char * lower = (char *)doublepage + PAGE_SIZE;

    IX_SlotDirectoryHeader header = getPageHeader(lower);
    Entry entry; // pivot entry
    // then find the pivot
    uint16_t i = 0; // pivot index
    for(; i < header.N; ++i) {
        entry = getEntry(i, lower);
        if (entry.offset >= PAGE_SIZE / 2) {
            memcpy(pivot, upper + entry.offset, entry.length - sizeof(RID)); // we don't need the rid
            break;
        }
    }

    // split pages
    // oldPage
    memcpy(oldPage, upper, entry.offset); // copy everything before the pivot to oldPage
    memcpy((char *)oldPage + PAGE_SIZE - header.N * sizeof(Entry) - sizeof(IX_SlotDirectoryHeader),
                     lower + PAGE_SIZE - header.N * sizeof(Entry) - sizeof(IX_SlotDirectoryHeader), 
                     header.N * sizeof(Entry)); // copy slotDir
    // update header
    IX_SlotDirectoryHeader oldHeader = header;
    oldHeader.N = i; // pivot isn't in the old page
    oldHeader.FS = entry.offset;
    setPageHeader(oldPage, oldHeader);

    // new page
    memcpy(newPage, upper + entry.offset, header.FS - entry.offset); // copy everything after the pivot to oldPage
    IX_SlotDirectoryHeader newHeader = header;
    newHeader.N = header.N - i;
    newHeader.FS = header.FS - entry.offset;
    memcpy((char *)newPage + PAGE_SIZE - sizeof(IX_SlotDirectoryHeader) - newHeader.N * sizeof(Entry),
            lower + PAGE_SIZE - sizeof(IX_SlotDirectoryHeader) - header.N * sizeof(Entry),
            newHeader.N * sizeof(Entry)); // copy slotDir
    setPageHeader(newPage, newHeader);
    upper = NULL; lower = NULL; free(doublepage);
}

void IndexManager::prepareDoublePage(const void * page, const Attribute &attribute, const void *key, const RID& rid, void * doublepage)
{
    // given a page without enough space and a (key, rid)
    // prepare a page with double size to hold the page and the (key, rid)
    char * upper = (char *)doublepage;
    char * lower = (char *)doublepage + PAGE_SIZE;

    // first copy everything to the double page
    IX_SlotDirectoryHeader header = getPageHeader(page);
    // copy all (key, rid) pairs to upper
    memcpy(upper, page, header.FS);
    // copy header and slotDir to lower
    memcpy(lower + PAGE_SIZE - header.N * sizeof(Entry) - sizeof(IX_SlotDirectoryHeader),
    (char *)page + PAGE_SIZE - header.N * sizeof(Entry) - sizeof(IX_SlotDirectoryHeader), 
                               header.N * sizeof(Entry) + sizeof(IX_SlotDirectoryHeader));
    
    // find the correct spot for inserted (key, rid)
    Entry entry;
    uint16_t i = 0;
    for (; i < header.N; ++i) {
        entry = getEntry(i, lower); // get entry from lower
        if (attribute.type == TypeInt) {
            int k;
            memcpy(&k, upper + entry.offset, INT_SIZE); // offset relates to upper
            if (k > *(int *)key) break;
        } else if (attribute.type == TypeReal) {
            float k;
            memcpy(&k, upper + entry.offset, REAL_SIZE); // offset relates to upper
            if (k > *(float *)key) break;
        } else if (attribute.type == TypeVarChar) {
            // current key
            int vclen;
            memcpy(&vclen, upper + entry.offset, 4); // offset relates to upper
            char k[vclen + 1];
            memcpy(k, upper + entry.offset + 4, vclen); // offset relates to upper
            k[vclen] = '\0';
            
            // search key
            int vclen2;
            memcpy(&vclen2, (char *)key, 4);
            char sk[vclen2 + 1];
            memcpy(sk, (char*)key + 4, vclen2);
            sk[vclen2] = '\0';

            if (strcmp(k, sk) > 0) break;
        }
    }
    int start = i == header.N ? header.FS : entry.offset; // start of the inserted (key, rid)
    int attrSize = getAttrSize(attribute, key);
    int length = attrSize + sizeof(RID); // length of the inserted (key, rid)
    int bytesToShift = header.FS - start;

    // shifting
    if (bytesToShift != 0) {
        memcpy(upper + start + length, (char *)page + start, bytesToShift);
        // update shifted's entries
        for (; i < header.N; ++i) {
            entry = getEntry(i, lower); // get entry from the lower half
            entry.offset += length;
            setEntry(i, entry, lower); // set entry to the lower half
        }
        // shift entries
        memcpy(lower + PAGE_SIZE - (header.N + 1) * sizeof(Entry),
              (char *)page + PAGE_SIZE - header.N * sizeof(Entry),
                                   (header.N - i) * sizeof(Entry));
    }

    // insert
    memcpy(upper + start, key, attrSize);
    // insert the rid
    memcpy(upper + start + attrSize, &rid, sizeof(RID));
    // new entry
    entry.offset = start;
    entry.length = length;
    setEntry(i, entry, lower); // set ith entry to the lower half
    // update header
    header.N += 1;
    header.FS += length;
    setPageHeader(lower, header); // update header in the lower half
}

void IndexManager::splitPages(void * oldPage, void* newPage, const Attribute &attribute, const void *key, int pointer, void * pivot)
{
    void * doublepage = malloc(2 * PAGE_SIZE);
    prepareDoublePage(oldPage, attribute, key, pointer, doublepage);

    char * upper = (char *)doublepage;
    char * lower = (char *)doublepage + PAGE_SIZE;

    IX_SlotDirectoryHeader header = getPageHeader(lower);
    Entry entry; // pivot entry
    // then find the pivot
    uint16_t i = 0; // pivot index
    for(; i < header.N; ++i) {
        entry = getEntry(i, lower);
        if (entry.offset >= PAGE_SIZE / 2) {
            memcpy(pivot, upper + entry.offset, entry.length - sizeof(int)); // we don't need the pointer
            break;
        }
    }

    // split pages
    // oldPage
    memcpy(oldPage, upper, entry.offset); // copy everything before the pivot to oldPage
    memcpy((char *)oldPage + PAGE_SIZE - header.N * sizeof(Entry) - sizeof(IX_SlotDirectoryHeader),
                     lower + PAGE_SIZE - header.N * sizeof(Entry) - sizeof(IX_SlotDirectoryHeader), 
                     header.N * sizeof(Entry)); // copy slotDir
    // update header
    IX_SlotDirectoryHeader oldHeader = header;
    oldHeader.N = i; // pivot isn't in the old page
    oldHeader.FS = entry.offset;
    setPageHeader(oldPage, oldHeader);

    // new page
    // pivot is not copied, only it's pointer is copied as new P0
    memcpy(newPage, upper + entry.offset + getAttrSize(attribute, pivot), header.FS - entry.offset); // copy everything after the pivot to oldPage
    IX_SlotDirectoryHeader newHeader = header;
    newHeader.N = header.N - i;
    newHeader.FS = header.FS - entry.offset;
    memcpy((char *)newPage + PAGE_SIZE - sizeof(IX_SlotDirectoryHeader) - newHeader.N * sizeof(Entry),
            lower + PAGE_SIZE - sizeof(IX_SlotDirectoryHeader) - header.N * sizeof(Entry),
            newHeader.N * sizeof(Entry)); // copy slotDir
    setPageHeader(newPage, newHeader);
    // set the 0th entry
    entry.offset = 0;
    entry.length = sizeof(int);
    setEntry(0, entry, newPage);
    upper = NULL; lower = NULL; free(doublepage);
}

void IndexManager::prepareDoublePage(const void * page, const Attribute &attribute, const void *key, int pointer, void * doublepage)
{
    // given a page without enough space and a (key, pointer)
    // prepare a page with double size to hold the page and the (key, pointer)
    char * upper = (char *)doublepage;
    char * lower = (char *)doublepage + PAGE_SIZE;

    // first copy everything to the double page
    IX_SlotDirectoryHeader header = getPageHeader(page);
    // copy all (key, pointer) pairs to upper
    memcpy(upper, page, header.FS);
    // copy header and slotDir to lower
    memcpy(lower + PAGE_SIZE - header.N * sizeof(Entry) - sizeof(IX_SlotDirectoryHeader),
    (char *)page + PAGE_SIZE - header.N * sizeof(Entry) - sizeof(IX_SlotDirectoryHeader), 
                               header.N * sizeof(Entry) + sizeof(IX_SlotDirectoryHeader));
    
    // find the correct spot for inserted (key, pointer)
    Entry entry;
    uint16_t i = 0;
    for (; i < header.N; ++i) {
        entry = getEntry(i, lower); // get entry from lower
        if (attribute.type == TypeInt) {
            int k;
            memcpy(&k, upper + entry.offset, INT_SIZE); // offset relates to upper
            if (k > *(int *)key) break;
        } else if (attribute.type == TypeReal) {
            float k;
            memcpy(&k, upper + entry.offset, REAL_SIZE); // offset relates to upper
            if (k > *(float *)key) break;
        } else if (attribute.type == TypeVarChar) {
            // current key
            int vclen;
            memcpy(&vclen, upper + entry.offset, 4); // offset relates to upper
            char k[vclen + 1];
            memcpy(k, upper + entry.offset + 4, vclen); // offset relates to upper
            k[vclen] = '\0';
            
            // search key
            int vclen2;
            memcpy(&vclen2, (char *)key, 4);
            char sk[vclen2 + 1];
            memcpy(sk, (char*)key + 4, vclen2);
            sk[vclen2] = '\0';

            if (strcmp(k, sk) > 0) break;
        }
    }
    int start = i == header.N ? header.FS : entry.offset; // start of the inserted (key, pointer)
    int attrSize = getAttrSize(attribute, key);
    int length = attrSize + sizeof(int); // length of the inserted (key, pointer)
    int bytesToShift = header.FS - start;

    // shifting
    if (bytesToShift != 0) {
        memcpy(upper + start + length, (char *)page + start, bytesToShift);
        // update shifted's entries
        for (; i < header.N; ++i) {
            entry = getEntry(i, lower); // get entry from the lower half
            entry.offset += length;
            setEntry(i, entry, lower); // set entry to the lower half
        }
        // shift entries
        memcpy(lower + PAGE_SIZE - (header.N + 1) * sizeof(Entry),
              (char *)page + PAGE_SIZE - header.N * sizeof(Entry),
                                   (header.N - i) * sizeof(Entry));
    }

    // insert
    memcpy(upper + start, key, attrSize);
    // insert the pointer
    memcpy(upper + start + attrSize, &pointer, sizeof(int));
    // new entry
    entry.offset = start;
    entry.length = length;
    setEntry(i, entry, lower); // set ith entry to the lower half
    // update header
    header.N += 1;
    header.FS += length;
    setPageHeader(lower, header); // update header in the lower half
}

int IndexManager::findPosition(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, void *page)
{
    // this function find the leaf page where the key should belong to
    // regardless if the key can be fit in
    // page number and the page are returned

    // start at the root page
    ixfileHandle.readPage(0, page); // read header page
    int curPageNum;
    memcpy(&curPageNum, page, sizeof(int)); // fetch root page number

    while (1) { // while you don't hit a leaf
        ixfileHandle.readPage(curPageNum, page);
        IX_SlotDirectoryHeader header = getPageHeader(page);
        if(header.leaf) return curPageNum; // if hit a leaf
        
        // |P0|K1|P1|K2|P2|...|slotDir|header|
        // read P0
        Entry entry = getEntry(0, page);
        int lastPointer;
        memcpy(&lastPointer, (char*)page + entry.offset, sizeof(int));

        for (uint16_t i = 1; i < header.N; ++i) { // loop through k-v pairs
            entry = getEntry(i, page); // get offset and length
            // find the first key larger than search key
            // then the search key should go to the last pointer
            if (attribute.type == TypeInt) {
                int k;
                memcpy(&k, (char*)page + entry.offset, INT_SIZE);
                if (k > *(int*)key) break;
                memcpy(&lastPointer, (char*)page + entry.offset + INT_SIZE, sizeof(int));
            } else if (attribute.type == TypeReal) {
                float k;
                memcpy(&k, (char*)page + entry.offset, REAL_SIZE);
                if (k > *(float*)key) break;
                memcpy(&lastPointer, (char*)page + entry.offset + REAL_SIZE, sizeof(int));
            } else if (attribute.type == TypeVarChar) {
                // current key
                int vclen;
                memcpy(&vclen, (char*)page + entry.offset, 4);
                char k[vclen + 1];
                memcpy(k, (char*)page + entry.offset + 4, vclen);
                k[vclen] = '\0';
                
                // search key
                int vclen2;
                memcpy(&vclen2, (char*)key, 4);
                char sk[vclen2 + 1];
                memcpy(sk, (char*)key + 4, vclen2);
                sk[vclen2] = '\0';

                if (strcmp(k, sk) > 0) break;
                memcpy(&lastPointer, (char*)page + entry.offset + 4 + vclen, sizeof(int));
            }
        }
        curPageNum = lastPointer;
    }
    return -1;
}

void IndexManager::insertEntryToPage(const Attribute &attribute, const void *key, const RID &rid, void *page)
{
    // insert entry (key, rid) to a leaf page
    Entry entry;
    int i = findKeyPos(attribute, key, entry, page, true); // index
    int start = entry.offset; // start of the the inserted (key, rid)
    int attrSize = getAttrSize(attribute, key);
    int length = attrSize + sizeof(RID); // length of the inserted (key, rid)
    IX_SlotDirectoryHeader header = getPageHeader(page);

    // shifting
    int bytesToShift = header.FS - start;
    if (bytesToShift != 0) {
        void * temp = malloc(bytesToShift);
        memcpy(temp, (char*)page + start, bytesToShift);
        // copy shifted bytes
        memcpy((char*)page + start + length, temp, bytesToShift);
        // update shifted's entries
        for (; i < header.N; ++i) {
            entry = getEntry(i, page);
            entry.offset += length;
            setEntry(i, entry, page);
        }
        free(temp);
        // shift entries
        bytesToShift = (header.N - i) * sizeof(Entry);
        void * temp2 = malloc(bytesToShift);
        memcpy(temp2, (char *)page + PAGE_SIZE - header.N * sizeof(Entry) - sizeof(IX_SlotDirectoryHeader), bytesToShift);
        memcpy((char *)page + PAGE_SIZE - (header.N + 1) * sizeof(Entry) - sizeof(IX_SlotDirectoryHeader), temp, bytesToShift);
        free(temp2);
    }
    // insert
    memcpy((char*)page + start, key, attrSize);
    // insert the rid
    memcpy((char*)page + start + attrSize, &rid, sizeof(RID));
    // new entry
    entry.offset = start;
    entry.length = length;
    setEntry(i, entry, page);
    // update header
    header.N += 1;
    header.FS += length;
    setPageHeader(page, header);
}

void IndexManager::insertEntryToPage(const Attribute &attribute, const void *key, int pointer, void *page)
{
    // insert entry (key, pointer) to a nonleaf page
    Entry entry;
    int i = findKeyPos(attribute, key, entry, page, false); // index
    int start = entry.offset; // start of the the inserted (key, pointer)
    int attrSize = getAttrSize(attribute, key);
    int length = attrSize + sizeof(int); // length of the inserted (key, pointer)
    IX_SlotDirectoryHeader header = getPageHeader(page);
    // shifting
    int bytesToShift = header.FS - start;
    if (bytesToShift != 0) {
        void * temp = malloc(bytesToShift);
        memcpy(temp, (char*)page + start, bytesToShift);
        // copy shifted bytes
        memcpy((char*)page + start + length, temp, bytesToShift);
        // update shifted's entries
        for (; i < header.N; ++i) {
            entry = getEntry(i, page);
            entry.offset += length;
            setEntry(i, entry, page);
        }
        free(temp);
        // shift entries
        bytesToShift = (header.N - i) * sizeof(Entry);
        void * temp2 = malloc(bytesToShift);
        memcpy(temp2, (char *)page + PAGE_SIZE - header.N * sizeof(Entry) - sizeof(IX_SlotDirectoryHeader), bytesToShift);
        memcpy((char *)page + PAGE_SIZE - (header.N + 1) * sizeof(Entry) - sizeof(IX_SlotDirectoryHeader), temp, bytesToShift);
        free(temp2);
    }
    // insert
    memcpy((char*)page + start, key, attrSize);
    // insert the pointer
    memcpy((char*)page + start + attrSize, &pointer, sizeof(int));
    // new entry
    entry.offset = start;
    entry.length = length;
    setEntry(i, entry, page);
    // update header
    header.N += 1;
    header.FS += length;
    setPageHeader(page, header);
}

int IndexManager::findKeyPos(const Attribute &attribute, const void *key, Entry& entry, const void *page, bool leaf)
{
    // this function find where a inserted key in a page should be
    // return entry of the first (key, rid) that's larger and it's index
    // if no such (key, rid), return entry.offset = header.FS and index = header.N
    IX_SlotDirectoryHeader header = getPageHeader(page);
    // leaf page format:
    // |K0|RID0|K1|RID1|...|slotDir|header|
    // non-leaf page format:
    // |P0|K1|P1|K2|P2|...|slotDir|header|
    // loop through to find the correct spot
    // find the first key that's larger than search key
    uint16_t i = leaf ? 0 : 1;
    for (; i < header.N; ++i) {
        entry = getEntry(i, page);
        if (attribute.type == TypeInt) {
            int k;
            memcpy(&k, (char*)page + entry.offset, INT_SIZE);
            if (k > *(int*)key) break;
        } else if (attribute.type == TypeReal) {
            float k;
            memcpy(&k, (char*)page + entry.offset, REAL_SIZE);
            if (k > *(float*)key) break;
        } else if (attribute.type == TypeVarChar) {
            // current key
            int vclen;
            memcpy(&vclen, (char*)page + entry.offset, 4);
            char k[vclen + 1];
            memcpy(k, (char*)page + entry.offset + 4, vclen);
            k[vclen] = '\0';
            
            // search key
            int vclen2;
            memcpy(&vclen2, (char*)key, 4);
            char sk[vclen2 + 1];
            memcpy(sk, (char*)key + 4, vclen2);
            sk[vclen2] = '\0';
            if (strcmp(k, sk) > 0) break;
        }
    }
    if(i == header.N) entry.offset = header.FS;
    return i;
}

// general helpers

int IndexManager::keyCompare(const Attribute& attr, const void * key1, const void * key2)
{
    switch (attr.type) {
        case TypeInt: 
            if(*(int *)key1 < *(int *)key2) return -1;
            else if(*(int *)key1 > *(int *)key2) return 1;
            else return 0;
        case TypeReal:
            if(*(float *)key1 < *(float *)key2) return -1;
            else if(*(float *)key1 > *(float *)key2) return 1;
            else return 0;
        case TypeVarChar:
        {
            int vclen;
            memcpy(&vclen, key1, 4);
            char key1str[vclen + 1];
            memcpy(key1str, (char *)key1 + sizeof(int), vclen);
            key1str[vclen]='\0';

            memcpy(&vclen, key2, 4);
            char key2str[vclen + 1];
            memcpy(key2str, (char *)key2 + sizeof(int), vclen);
            key2str[vclen]='\0';

            return strcmp(key1str, key2str);
        }
    }
    return -1;
}

void IndexManager::setRoot(IXFileHandle &ixfileHandle, int pageNum)
{
    void * page = malloc(PAGE_SIZE);
    ixfileHandle.readPage(0, page);
    memcpy(page, &pageNum, sizeof(int));
    ixfileHandle.writePage(0, page);
    free(page);
}

void IndexManager::initIXfile(const Attribute& attr, IXFileHandle &ixfileHandle)
{
    // this function store the header info in page 0 
    // and create an empty root page (page 1)

    // header page format:
    // |rootPageNum(4B)|ixAttribute(variable length)|
    // assume ixAttribute can be fit in a page
    void * page = malloc(PAGE_SIZE);
    int offset = 0;

    int rootPageNum = 1;
    memcpy((char *)page, &rootPageNum, sizeof(int));
    offset += sizeof(int);

    int namelen = attr.name.size();
    memcpy((char *)page + offset, &namelen, sizeof(int));
    offset += sizeof(int);

    memcpy((char *)page + offset, attr.name.c_str(), namelen);
    offset += namelen;

    memcpy((char *)page + offset, &attr.type, sizeof(AttrType));
    offset += sizeof(AttrType);

    memcpy((char *)page + offset, &attr.length, sizeof(AttrLength));

    // flush it to file
    ixfileHandle.appendPage(page);

    // empty root page
    // root is a leaf at the beginning
    // non-leaf page format:
    // |P0|K1|P1|K2|P2|...|slotDir|header|
    // leaf page format:
    // |K0|RID0|K1|RID1|...|slotDir|header|
    IX_SlotDirectoryHeader header;
    header.FS = 0;
    header.N = 0;
    header.leaf = 1; // yes it's a leaf
    header.next = LEAF_END;
    header.parent = 0; // the header page, for root only
    setPageHeader(page, header);
    ixfileHandle.appendPage(page);
    free(page);
}

bool IndexManager::checkIXAttribute(const Attribute& attr, IXFileHandle &ixfileHandle)
{
    // obain the header page
    void * page = malloc(PAGE_SIZE);
    ixfileHandle.readPage(0, page);
    int offset = 4;
    int namelen;
    memcpy(&namelen, (char *)page + offset, sizeof(int));
    offset += sizeof(int);

    char name[namelen + 1];
    memcpy(name, (char *)page + offset, namelen);
    offset += namelen;
    name[namelen] = '\0';

    AttrType type;
    memcpy(&type, (char *)page + offset, sizeof(AttrType));
    offset += sizeof(AttrType);

    AttrLength length;
    memcpy(&length, (char *)page + offset, sizeof(AttrLength));

    free(page);

    return string(name) == attr.name && type == attr.type && length == attr.length;
}

int IndexManager::getPageFreeSpaceSize(const void * page)
{
    IX_SlotDirectoryHeader header = getPageHeader(page);
    return (PAGE_SIZE - header.FS - header.N * sizeof(Entry) - sizeof(IX_SlotDirectoryHeader));
}

IX_SlotDirectoryHeader IndexManager::getPageHeader(const void * page)
{
    IX_SlotDirectoryHeader header;
    memcpy(&header, (char *)page + PAGE_SIZE - sizeof(IX_SlotDirectoryHeader), sizeof(IX_SlotDirectoryHeader));
    return header;
}

void IndexManager::setPageHeader(void * page, const IX_SlotDirectoryHeader& header)
{
    memcpy((char *)page + PAGE_SIZE - sizeof(IX_SlotDirectoryHeader), &header, sizeof(IX_SlotDirectoryHeader));
}

int IndexManager::getAttrSize(const Attribute &attribute, const void *key)
{
    switch (attribute.type) {
        case TypeInt: return INT_SIZE;
        case TypeReal: return REAL_SIZE;
        case TypeVarChar:
        {
            int vclen;
            memcpy(&vclen, key, 4);
            return (vclen + 4);
        }
    }
    return -1;
}

Entry IndexManager::getEntry(const int i, const void * page)
{
    // return the ith entry
    // i start at 0
    // format:
    // |...|E2|E1|E0|Header|
    // start of E1 = start + PAGE_SIZE - HeaderSize - (1 + 1) * EntrySize
    // start of Ei = start + PAGE_SIZE - HeaderSize - (i + 1) * EntrySize
    Entry entry;
    memcpy(&entry, (char*)page + PAGE_SIZE - sizeof(IX_SlotDirectoryHeader) - (i + 1) * sizeof(entry), sizeof(entry));
    return entry;
}

void IndexManager::setEntry(const int i, const Entry entry, void * page)
{
    // i start at 0
    memcpy((char*)page + PAGE_SIZE - sizeof(IX_SlotDirectoryHeader) - (i + 1) * sizeof(entry), &entry, sizeof(entry));
}


//***********************************************************************************************************************************
//                                                                DELETE                                                           **
//***********************************************************************************************************************************
RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    // check the attribute
    int numOfPage = ixfileHandle.getNumberOfPages();
    if (numOfPage == 0) { // delete before insert
        return IX_ATTR_DN_EXIST;
    } else {
        // check attribute
        if (checkIXAttribute(attribute, ixfileHandle)) return IX_ATTR_MISMATCH;
    }
    void * page = malloc(PAGE_SIZE);
    // find the leaf page it belongs to
    int targetPageNum = findPosition(ixfileHandle, attribute, key, page);

    const IX_SlotDirectoryHeader header = getPageHeader(page);
    if (!header.leaf) return -1; // this has to be a leaf page

    Entry entry;
    RID crid;
    uint16_t i = 0;
    for (; i < header.N; ++i) {
        entry = getEntry(i, page);
        if (attribute.type == TypeInt) {
            int k;
            memcpy(&k, (char*)page + entry.offset, INT_SIZE);
            memcpy(&crid, (char*)page + entry.offset + INT_SIZE, sizeof(crid));
            if (k == *(int*)key && crid.pageNum == rid.pageNum && crid.slotNum == rid.slotNum) {
                crid.pageNum = -1;
                crid.slotNum = -1;
                memcpy((char*)page + entry.offset + INT_SIZE, &crid, sizeof(RID));
                ixfileHandle.writePage(targetPageNum, page);
                free(page);
                return SUCCESS;
            }
        } else if (attribute.type == TypeReal) {
            float k;
            memcpy(&k, (char*)page + entry.offset, REAL_SIZE);
            memcpy(&crid, (char*)page + entry.offset + REAL_SIZE, sizeof(RID));
            if (k == *(float*)key && crid.pageNum == rid.pageNum && crid.slotNum == rid.slotNum) {
                crid.pageNum = -1;
                crid.slotNum = -1;
                memcpy((char*)page + entry.offset + REAL_SIZE, &crid, sizeof(RID));
                ixfileHandle.writePage(targetPageNum, page);
                free(page);
                return SUCCESS;
            }
        } else if (attribute.type == TypeVarChar) {
            // current key
            int vclen;
            memcpy(&vclen, (char*)page + entry.offset, 4);
            char k[vclen + 1];
            memcpy(k, (char*)page + entry.offset + 4, vclen);
            k[vclen] = '\0';

            // current rid
            memcpy(&crid, (char*)page + entry.offset + 4 + vclen, sizeof(RID));

            // search key
            int vclen2;
            memcpy(&vclen2, (char*)key, 4);
            char sk[vclen2 + 1];
            memcpy(sk, (char*)key + 4, vclen2);
            sk[vclen2] = '\0';

            if (strcmp(k, sk) == 0 && crid.pageNum == rid.pageNum && crid.slotNum == rid.slotNum) {
                crid.pageNum = -1;
                crid.slotNum = -1;
                memcpy((char*)page + entry.offset + 4 + vclen, &crid, sizeof(RID));
                ixfileHandle.writePage(targetPageNum, page);
                free(page);
                return SUCCESS;
            }
        }
    }
    return IX_ATTR_DN_EXIST;
}

bool IndexManager::fileExists(const string &fileName)
{
    // If stat fails, we can safely assume the file doesn't exist
    struct stat sb;
    return stat(fileName.c_str(), &sb) == 0;
}


//***********************************************************************************************************************************
//                                                                SCAN                                                             **
//***********************************************************************************************************************************
RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
    return ix_ScanIterator.scanInit(ixfileHandle, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive);
}


//***********************************************************************************************************************************
//                                                                PRINT                                                            **
//***********************************************************************************************************************************
void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
    int numOfPage = ixfileHandle.getNumberOfPages();
    if (numOfPage == 0) { nothing there
        cout << "{}" << endl;
        return;
    } else {
        // check attribute
        if (checkIXAttribute(attribute, ixfileHandle)){
            cout << "Error: Attribute mismatched" << endl;
            return;
        }
    }
    void * page = malloc(PAGE_SIZE);
    ixfileHandle.readPage(0, page);
    int root;
    memcpy(&root, page, sizeof(int));
    free(page);
    DFSPrint(ixfileHandle, attribute, root, 0);
}

void IndexManager::DFSPrint(IXFileHandle &ixfileHandle, const Attribute &attribute, int curPage, int depth)
{
    /*
    void * page = malloc(PAGE_SIZE);
    ixfileHandle.readPage(curPage, page);
    IX_SlotDirectoryHeader header = getPageHeader(page);
    if(header.leaf == 1) {
        for(uint16_t i = 0; i < header.N; ++i) {
            Entry entry = getEntry(i, page);
            switch (attribute.type) {
                case TypeInt: 
                {
                    int key;
                    memcpy(&key, (char *)page + entry.offset, INT_SIZE);
                    RID rid;
                    memcpy(&rid, (char *)page + entry.offset + INT_SIZE, sizeof(RID));
                    cout << "\"" << key << " : [(" << rid.pageNum << ", " << rid.slotNum << ")]"; 
                    if (i != header.N) << ",";
                }
                case TypeReal: 
                {
                    float key;
                    memcpy(&key, (char *)page + entry.offset, REAL_SIZE);
                    RID rid;
                    memcpy(&rid, (char *)page + entry.offset + REAL_SIZE, sizeof(RID));
                    cout << "\"" << key << " : [(" << rid.pageNum << ", " << rid.slotNum << ")]"; 
                    if (i != header.N) << ",";
                }
                case TypeVarChar:
                {
                    int vclen;
                    memcpy(&vclen, (char *)page + entry.offset, 4);
                    return (vclen + 4);
                    char key[vclen + 1];
                    memcpy(key, (char *)page + entry.offset + 4, vclen);
                    key[vclen] = '\0';
                    RID rid;
                    memcpy(&rid, (char *)page + entry.offset + 4 + vclen, sizeof(RID));
                    cout << "\"" << key << " : [(" << rid.pageNum << ", " << rid.slotNum << ")]"; 
                    if (i != header.N) << ",";
                }
            }
        }
        cout << endl;
    } else {
        vector<int> pointers;
        cout << "{\"keys\" : ["
        for(uint16_t i = 0; i < header.N; ++i) {
            Entry entry = getEntry(i, page);
            switch (attribute.type) {
                case TypeInt: 
                {
                    int key;
                    memcpy(&key, (char *)page + entry.offset, INT_SIZE);
                    int pointer;
                    memcpy(&pointer, (char *)page + entry.offset + INT_SIZE, sizeof(int));
                    pointers.push_back(pointer);
                    cout << "\"" << key << "\""; 
                    if (i != header.N) << ",";
                }
                case TypeReal: 
                {
                    float key;
                    memcpy(&key, (char *)page + entry.offset, REAL_SIZE);
                    int pointer;
                    memcpy(&pointer, (char *)page + entry.offset + REAL_SIZE, sizeof(int));
                    pointers.push_back(pointer);
                    cout << "\"" << key << "\""; 
                    if (i != header.N) << ",";
                }
                case TypeVarChar:
                {
                    int vclen;
                    memcpy(&vclen, key, 4);
                    return (vclen + 4);
                    char key[vclen + 1];
                    memcpy(key, (char *)page + entry.offset, vclen);
                    key[vclen] = '\0';
                    int pointer;
                    memcpy(&pointer, (char *)page + entry.offset + 4 + vclen, sizeof(int));
                    pointers.push_back(pointer);
                    cout << "\"" << key << "\"";
                    if (i != header.N) << ",";
                }
            }
        }
        cout << "]," << endl;
        for (size_t j = 0; j < pointers.size(); ++i) {
            cout << "\"children\": ["
            DFSPrint(ixfileHandle, attribute, pointers[j], ++depth);
            cout << "]}"
        }
    }
    */
}

// for debugging
void IndexManager::printPage(IXFileHandle &ixfileHandle, const Attribute &attribute, int pageNum)
{
    
}

//***********************************************************************************************************************************
//                                                        SCAN_ITERATOR_CLASS                                                      **
//***********************************************************************************************************************************
IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    return -1;
}

RC IX_ScanIterator::close()
{
    return -1;
}

RC IX_ScanIterator::scanInit(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive)
{
    this->ixfileHandle = ixfileHandle;
    this->attribute = attribute;
    this->lowKey = lowKey;
    this->highKey = highKey;
    this->lowKeyInclusive = lowKeyInclusive;
    this->highKeyInclusive = highKeyInclusive;
    return SUCCESS;
}


//***********************************************************************************************************************************
//                                                        FILE_HANDLE_CLASS                                                        **
//***********************************************************************************************************************************
IXFileHandle::IXFileHandle()
{
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
    _fd = nullptr;
}

IXFileHandle::~IXFileHandle()
{
    // use closeFile
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    readPageCount = ixReadPageCounter;
    writePageCount = ixWritePageCounter;
    appendPageCount = ixAppendPageCounter;
    return SUCCESS;
}

RC IXFileHandle::readPage(PageNum pageNum, void *data)
{
    // If pageNum doesn't exist, error
    if (getNumberOfPages() <= pageNum)
        return FH_PAGE_DN_EXIST;

    // Try to seek to the specified page
    if (fseek(_fd, PAGE_SIZE * pageNum, SEEK_SET))
        return FH_SEEK_FAILED;

    // Try to read the specified page
    if (fread(data, 1, PAGE_SIZE, _fd) != PAGE_SIZE)
        return FH_READ_FAILED;

    ixReadPageCounter++;
    return SUCCESS;
}


RC IXFileHandle::writePage(PageNum pageNum, const void *data)
{
    // Check if the page exists
    if (getNumberOfPages() <= pageNum)
        return FH_PAGE_DN_EXIST;

    // Seek to the start of the page
    if (fseek(_fd, PAGE_SIZE * pageNum, SEEK_SET))
        return FH_SEEK_FAILED;

    // Write the page
    if (fwrite(data, 1, PAGE_SIZE, _fd) == PAGE_SIZE)
    {
        // Immediately commit changes to disk
        fflush(_fd);
        ixWritePageCounter++;
        return SUCCESS;
    }
    
    return FH_WRITE_FAILED;
}


RC IXFileHandle::appendPage(const void *data)
{
    // Seek to the end of the file
    if (fseek(_fd, 0, SEEK_END))
        return FH_SEEK_FAILED;

    // Write the new page
    if (fwrite(data, 1, PAGE_SIZE, _fd) == PAGE_SIZE)
    {
        fflush(_fd);
        ixAppendPageCounter++;
        return SUCCESS;
    }
    return FH_WRITE_FAILED;
}


unsigned IXFileHandle::getNumberOfPages() // +4 doesn't change anything
{
    // Use stat to get the file size
    struct stat sb;
    if (fstat(fileno(_fd), &sb) != 0)
        // On error, return 0
        return 0;
    // Filesize is always PAGE_SIZE * number of pages
    return sb.st_size / PAGE_SIZE;
}

void IXFileHandle::setfd(FILE *fd)
{
    _fd = fd;
}

FILE *IXFileHandle::getfd()
{
    return _fd;
}
