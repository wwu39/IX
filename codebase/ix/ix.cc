
#include "ix.h"
#include <sys/stat.h>
#include <cstring>
#include <string.h>

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
    int targetPage = findPosition(attribute, key, page);
    int FSSzie = getPageFreeSpaceSize(page);
    int attrSize = getAttrSize(attribute, key);
    if (attrSize + (int)sizeof(Entry) <= FSSzie) { // can be fit in

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
    // |K1|RID1|K2|RID2|...|slotDir|header|
    IX_SlotDirectoryHeader header;
    header.FS = 0;
    header.N = 0;
    header.leaf = 1; // yes it's a leaf
    header.next = LEAF_END;
    offset = PAGE_SIZE - sizeof(IX_SlotDirectoryHeader);
    memcpy((char *)page + offset, &header, sizeof(IX_SlotDirectoryHeader));
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
    IX_SlotDirectoryHeader header;
    memcpy(&header, (char *)page + PAGE_SIZE - sizeof(IX_SlotDirectoryHeader), sizeof(IX_SlotDirectoryHeader));
    return (PAGE_SIZE - header.FS - header.N * sizeof(Entry) - sizeof(IX_SlotDirectoryHeader));
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

int IndexManager::findPosition(const Attribute &attribute, const void *key, void *page)
{
    // this function find the page where the key should belong to
    // regardless if the key can be fit in
    return -1;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    return -1;
}

bool IndexManager::fileExists(const string &fileName)
{
    // If stat fails, we can safely assume the file doesn't exist
    struct stat sb;
    return stat(fileName.c_str(), &sb) == 0;
}

//-- SCAN FUNCTION -------------------------------------------------------------------------------------------
//------------------------------------------------------------------------------------------------------------
//------------------------------------------------------------------------------------------------------
//Changes added to design :
//I added a new field in the slotDirectoryHeader struct to keep track of the the page before in the linked list

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    void* page = malloc(PAGE_SIZE);                             // Read the currentPage. All of this was initialized by the SCAN function
    if(ixfileHandle.readPage(this->currentPage,page))
        return FH_READ_FAILED;   

    IX_SlotDirectoryHeader currentPageHeader = IndexManager::getPageHeader(page);

    if(currentPageHeader.N < this->entry_number_low_key)   // entry_number_low_key keeps track of where we are in the page. This field was initialized by SCAN
    {
        this->entry_number_low_key = 1;        // if N<entry_number_low_key, it means we crossed the boundary. so we reset entry_number_low_key
        this->currentPage = currentPageHeader.next; //go to the next page
        this->currentEntryOffset = 0; // reset the currentEntryOffset
    }

    if(this->currentPage == this->highPageNum) // if we are in the last page of the range search
      {
            if(this->entry_number_low_key > this->entry_number_high_key ) // if we crossed he boundary regarding the last page, return
                return SUCCESS;
      }
   
        if(readEntry(rid,key))   // read Entry. This functions sets up all variables for the next iteration
            return FH_READ_FAILED;

        this->entry_number_low_key++;   //next key
    
    
    return SUCCESS;
}


RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
    if (checkIXAttribute(attribute, ixfileHandle) < 0) 
        return IX_ATTR_MISMATCH;

    return ix_ScanIterator.scanInit(ixfileHandle, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive);

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




    switch(attribute.type)// Make sure lowKey<=HighKey
    {
        case TypeInt:
        uint32_t int_lowKey;
        uint32_t int_highKey;
        memcpy(&int_lowKey,lowKey,INT_SIZE);
        memcpy(&int_highKey,highKey,INT_SIZE);

        if(lowKey > highKey)
            return -1;
        break;

        case TypeReal:
        float float_lowKey;
        float float_highKey;

        memcpy(&float_lowKey,lowKey,REAL_SIZE);
        memcpy(&float_highKey,highKey,REAL_SIZE);

        if(float_lowKey > float_highKey)
            return -1;
        break;

        case TypeVarChar:
        uint32_t varcharSize_lowKey;
        uint32_t varcharSize_highKey;
        memcpy(&varcharSize_lowKey,lowKey,VARCHAR_LENGTH_SIZE);
        memcpy(&varcharSize_highKey,highKey,VARCHAR_LENGTH_SIZE);

        char *data_string_lowKey = (char*) malloc(varcharSize_lowKey + 1);
        char *data_string_highKey = (char*) malloc(varcharSize_highKey + 1);

        memcpy(data_string_lowKey,lowKey,varcharSize_lowKey);
        memcpy(data_string_highKey,highKey,varcharSize_lowKey);

        data_string_lowKey[varcharSize_lowKey] = '\0';
        data_string_highKey[varcharSize_highKey] = '\0';

        if(strcmp(data_string_lowKey, data_string_highKey)>0)
            return -1;
        break;

    } 

    if(setLowKeyPageNum() != 0 || setHighKeyPageNum() != 0)
        return -1;

    return SUCCESS;
}




 int IX_ScanIterator::setLowKeyPageNum()
 {
     if(this->ixfileHandle.getNumberOfPages()== 0)
        return -1; // There is no pages

     int rootPageNum = IndexManager::getRoot(this->ixfileHandle);
     void * data = malloc(PAGE_SIZE);
     if(this->ixfileHandle.readPage(rootPageNum,data))
        return RBFM_READ_FAILED;

    void * page = malloc(PAGE_SIZE);
     if(this->lowKey != NULL)
     {
         if(this->currentPage = IndexManager::findPosition(ixfileHandle,attribute,this->lowKey,page)<0) // find the page where the key lives
            return IX_KEY_NOT_FOUND;

         if(searchKey(this->lowKey,this->lowKeyInclusive,this->currentEntryOffset,this->entry_number_low_key,page,0)) // searchKey in the page and sets up the iterative variable properly
            return IX_KEY_NOT_FOUND; // KEY NOT FOUND
     }
     else{
         if(searchLeftEnd())  // Find the left end and sets up the iterative variables properly
            return IX_KEY_NOT_FOUND;
     }
    
    return SUCCESS;
 }

 int IX_ScanIterator::setHighKeyPageNum()
 {
     if(this->ixfileHandle.getNumberOfPages()== 0)
        return -1; // There is no pages

     int rootPageNum = IndexManager::getRoot(this->ixfileHandle);
     void * data = malloc(PAGE_SIZE);
     if(this->ixfileHandle.readPage(rootPageNum,data))
        return RBFM_READ_FAILED;

     if(this->highKey != NULL)
     {
          void * page = malloc(PAGE_SIZE);
          if(this->highPageNum = IndexManager::findPosition(ixfileHandle,attribute,this->highKey,page)<0)
            return IX_KEY_NOT_FOUND;

         if(searchKey(this->highKey,this->highKeyInclusive,this->limitOffset,this->entry_number_high_key,page,1))
            return IX_KEY_NOT_FOUND; // KEY NOT FOUND
     }
     else{
         if(searchRightEnd()) // find right end and sets up the iterative variables properly
            return IX_KEY_NOT_FOUND;
     }
    
    return SUCCESS;
 }

 int IX_ScanIterator::searchKey(const void* key, bool inclusive, int &offset, int &entry_number, void* page, int leftOrRight)  // search key in the page
 {
     IX_SlotDirectoryHeader currentPageHeader = IndexManager::getPageHeader(page);
     int current_offset = 0;
     bool key_exists = false;
     
     for(unsigned i=1; i<=currentPageHeader.N;i++) // iterate thorugh all the entries in the page
     {

       if(this->attribute.type == TypeInt) // check for the attribute
        {
        uint32_t int_key;
        uint32_t current_int_key;
        memcpy(&int_key,key,INT_SIZE);  //get the actual value of key
        memcpy(&current_int_key,(char *)page+current_offset,INT_SIZE); // get the actual value of the current key in the page

        if(int_key == current_int_key)  // if both keys match then we found it
        {
            entry_number = i; // where it is
            key_exists = true; // it exists
            offset = current_offset; // where exactly it is
            if(inclusive == false) // if it's exclusive
                {
                    handleInclusion(key,current_offset,offset,entry_number,page,leftOrRight); //handle exclusion
                }

            break;
        }
        current_offset = current_offset + INT_SIZE + sizeof(RID); // iterate thorugh the page. Get the address of the next entry

        }
        else if(this->attribute.type == TypeReal)  // ->> similar analysis as before
         {
             float float_key;
             float current_float_key;

             memcpy(&float_key,key,REAL_SIZE);
             memcpy(&current_float_key,(char *)page+current_offset,REAL_SIZE);

             if(float_key == current_float_key)
             {
                 entry_number = i;
                 key_exists = true;
                 offset = current_offset;
                 if(inclusive == false)
                 {
                    handleInclusion(key,current_offset,offset,entry_number,page,leftOrRight);
                 }

                 break;
             }
        
             current_offset = current_offset + REAL_SIZE + sizeof(RID);
         }
        else if(this->attribute.type == TypeVarChar)
         {
             uint32_t var_size_key;
             uint32_t var_size_current_key;

             memcpy(&var_size_key,key,4);
             memcpy(&var_size_current_key,(char *)page+current_offset,4);

             char* char_key = (char*)malloc(var_size_key+1);
             char* current_char_key = (char *)malloc(var_size_current_key+1);

             memcpy(char_key,(char*)key+4,var_size_key);
             memcpy(current_char_key,(char *)page+current_offset+4,var_size_current_key);

             char_key[var_size_key] = '\0';
             current_char_key[var_size_current_key] = '\0';

             if(strcmp(char_key,current_char_key)==0)
             {
                 entry_number = i;
                 key_exists = true;
                 offset = current_offset;
                 if(inclusive == false)
                 {
                     handleInclusion(key,current_offset,offset,entry_number,page,leftOrRight);
                 }
                 break;
             }
             
             current_offset = current_offset + 4 + var_size_current_key + sizeof(RID);
         }

     }

     if(key_exists == false)
        return -1;
     

     return SUCCESS;
 }

 int IX_ScanIterator::searchLeftEnd()
 {
     return -1;
 }

 int IX_ScanIterator::searchRightEnd()
 {
     return -1;
 }

RC IX_ScanIterator::readEntry(RID &rid, void *key)
{
    void* data = malloc(PAGE_SIZE);

    if(this->ixfileHandle.readPage(this->currentPage,data))
        return RBFM_READ_FAILED;

    if(this->attribute.type == TypeInt)
    {
        memcpy(key,(char *)data+currentEntryOffset,INT_SIZE);
        currentEntryOffset+=INT_SIZE;
        memcpy(&rid,(char *)data+currentEntryOffset,sizeof(RID));
        currentEntryOffset+=sizeof(RID);
    }
    else if(this->attribute.type == TypeReal)
    {
        memcpy(key,(char *)data+currentEntryOffset,REAL_SIZE);
        currentEntryOffset += REAL_SIZE;
        memcpy(&rid,(char *)data+currentEntryOffset,sizeof(RID));
        currentEntryOffset += sizeof(RID);

    }
    else if(this->attribute.type == TypeVarChar)
    {
        uint32_t varcharSize;
        memcpy(&varcharSize,(char *)data+currentEntryOffset,INT_SIZE);
        currentEntryOffset+=INT_SIZE;
        memcpy(key,(char *)data+currentEntryOffset,varcharSize);
        currentEntryOffset+=varcharSize;
        memcpy(&rid,(char *)data+currentEntryOffset,sizeof(RID));
        currentEntryOffset+=sizeof(RID);
    }
}

int IX_ScanIterator::handleInclusion(const void* key, int currentOffset,int &offset, int &entry_number, void* page, int leftOrRight)
{
    return -1;
}


int IndexManager::getRoot(IXFileHandle &ixfileHandle)
{
    int rootPageNum;

    void *data = malloc(PAGE_SIZE);
    ixfileHandle.readPage(0,data);
    memcpy(&rootPageNum,data,4);

    free(data);
    return rootPageNum;

}

 //-----------------------------------------------------------------------------------------------------
 //----------------------------------------------------------------------------------------------------
 //------------------------------------------------------------------------------------------------

 int IndexManager::findPosition(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, void *page)
{

    return -1;
}






//-------------------------------
void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
}

IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}



RC IX_ScanIterator::close()
{
    return -1;
}


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


//------- helper functions created by Daniel


IX_SlotDirectoryHeader IndexManager::getPageHeader(const void * page)
{
    IX_SlotDirectoryHeader header;
    memcpy(&header, (char *)page + PAGE_SIZE - sizeof(IX_SlotDirectoryHeader), sizeof(IX_SlotDirectoryHeader));
    return header;
}





