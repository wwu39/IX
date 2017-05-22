
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
    string ixfile = fileName + ".ix";
    // If the file already exists, error
    if (fileExists(ixfile))
        return IX_FILE_EXISTS;

    // Attempt to open the file for writing
    FILE *pFile = fopen(ixfile.c_str(), "wb");
    // Return an error if we fail
    if (pFile == NULL)
        return IX_OPEN_FAILED;
    
    fclose (pFile);
    return SUCCESS;
}

RC IndexManager::destroyFile(const string &fileName)
{
    string ixfile = fileName + ".ix";
    if (remove(ixfile.c_str()) != 0)
        return IX_REMOVE_FAILED;
    return SUCCESS;
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
    // If this handle already has an open file, error
    if (ixfileHandle.getfd() != NULL)
        return IX_HANDLE_IN_USE;

    string ixfile = fileName + ".ix";

    // If the file doesn't exist, error
    if (!fileExists(ixfile.c_str()))
        return PFM_FILE_DN_EXIST;
    
    // Open the file for reading/writing in binary mode
    FILE *pFile;
    pFile = fopen(ixfile.c_str(), "rb+");
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
        if (checkIXAttribute(attribute, ixfileHandle)) return IX_ATTR_MISMATCH;
    }
    void * page = malloc(PAGE_SIZE);
    // find the leaf page it belongs to
    int targetPageNum = findPosition(ixfileHandle, attribute, key, page);
    int FSSzie = getPageFreeSpaceSize(page);
    int attrSize = getAttrSize(attribute, key);
    if (attrSize + (int)sizeof(Entry) <= FSSzie) { // can be fit in
        insertEntryToPage(attribute, key, rid, page);
        ixfileHandle.writePage(targetPageNum, page);
    } else { // must split

    }
    free(page);
    return SUCCESS;
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

    unsigned rootPageNum = 1;
    memcpy((char *)page, &rootPageNum, sizeof(unsigned));
    offset += sizeof(unsigned);

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

int IndexManager::findPosition(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, void *page)
{
    // this function find the leaf page where the key should belong to
    // regardless if the key can be fit in
    // page number is returned

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
    // insert entry to a page in the proper position
    // all entries greater then the inserted will be shifted
    IX_SlotDirectoryHeader header = getPageHeader(page);
    if (!header.leaf) cout << "insertEntryToPage: Not a leaf" << endl; // this has to be a leaf page
    // leaf page format:
    // |K0|RID0|K1|RID1|...|slotDir|header|
    Entry entry;
    // loop through to find the correct spot
    // find the first key that's larger than search key
    // that key and anything behind are shifted
    uint16_t i = 0;
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
    // where does the new entry start?
    // if can't find larger key, it starts at FS
    // if there is a larger key, it starts at where the large key starts
    int start = i == header.N ? header.FS : entry.offset;
    int attrSize = getAttrSize(attribute, key);
    int length = attrSize + sizeof(RID); // length of the inserted (key, rid)

    // shifting
    int bytesToShift = header.FS - start;
    if (bytesToShift != 0) {
        void * temp = malloc(bytesToShift);
        memcpy(temp, (char*)page + start, bytesToShift);
        // copy shifted bytes
        memcpy((char*)page + start + length, temp, bytesToShift);
        // update shifted's entries
        for (; i < header.N; ++i) { // N has incremented
            entry = getEntry(i, page);
            entry.offset += length;
            setEntry(i, entry, page);
        }
        free(temp);
    }
    // insert
    memcpy((char*)page + start, key, attrSize);
    // insert the rid
    memcpy((char*)page + start + attrSize, &rid, sizeof(RID));
    // new entry
    entry.offset = start;
    entry.length = length;
    setEntry(header.N, entry, page);
    // update header
    header.N += 1;
    header.FS += length;
    setPageHeader(page, header);
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
