#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <climits>

#include "../rbf/rbfm.h"

#define IX_EOF (-1)  // end of the index scan
#define LEAF_END (-1) // end of consecutive leaves

#define IX_FILE_EXISTS   1
#define IX_OPEN_FAILED   2
#define IX_REMOVE_FAILED 3
#define IX_HANDLE_IN_USE 4
#define IX_FILE_DN_EXIST 5
#define IX_FILE_NOT_OPEN 6

#define IX_ATTR_MISMATCH 7
#define IX_ATTR_DN_EXIST 8

typedef struct
{
    uint16_t FS; // free space pointer
    uint16_t N; // number of k-v pairs
    uint8_t leaf; // is this page a leaf page? 0 = no
    int32_t next; // if it's a leaf page, what's the next leaf?
    int32_t parent; // should have parent. root's parent is 0
} IX_SlotDirectoryHeader;

typedef struct
{
    uint16_t length; 
    uint16_t offset;
} Entry;


class IX_ScanIterator;
class IXFileHandle;

class IndexManager {
    friend class IX_ScanIterator;

    public:
        static IndexManager* instance();

        // Create an index file.
        RC createFile(const string &fileName);

        // Delete an index file.
        RC destroyFile(const string &fileName);

        // Open an index and return an ixfileHandle.
        RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

        // Close an ixfileHandle for an index.
        RC closeFile(IXFileHandle &ixfileHandle);

        // Insert an entry into the given index that is indicated by the given ixfileHandle.
        RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixfileHandle.
        RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute);

        // for debug
        void printPage(IXFileHandle &ixfileHandle, const Attribute &attribute, int pageNum);
        void printDoublePage(const Attribute &attribute, void * page);

    protected:
        IndexManager();
        ~IndexManager();

    private:
        static IndexManager *_index_manager;
        // Private helper methods
        bool fileExists(const string &fileName);
        void initIXfile(const Attribute& attr, IXFileHandle &ixfileHandle);
        bool checkIXAttribute(const Attribute& attr, IXFileHandle &ixfileHandle);
        int findPosition(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, void *page);
        int smallestLeaf(IXFileHandle &ixfileHandle, const Attribute &attribute, void *page);

        // insert helper
        void insertEntryToPage(const Attribute &attribute, const void *key, const RID &rid, void *page);
        void insertEntryToPage(const Attribute &attribute, const void *key, int pointer, void *page);
        int findKeyPos(const Attribute &attribute, const void *key, Entry& entry, const void *page, bool leaf);
        void prepareDoublePage(const void * page, const Attribute &attribute, const void *key, const RID& rid, void * doublepage);
        void prepareDoublePage(const void * page, const Attribute &attribute, const void *key, int pointer, void * doublepage);
        void splitPages(void * oldPage, void* newPage, const Attribute &attribute, const void *key, const RID &rid, void * pivot);
        void splitPages(void * oldPage, void* newPage, const Attribute &attribute, const void *key, int pointer, void * pivot);
        void splitAncestors(int leftChild, const Attribute &attribute, const void * pivot, int pointer, IXFileHandle &ixfileHandle, const int parent);

        // print helper
        void DFSPrint(IXFileHandle &ixfileHandle, const Attribute &attribute, int curPage, int depth);

        // general helper
        int keyCompare(const Attribute& attr, const void * key1, const void * key2);
        void setRoot(IXFileHandle &ixfileHandle, int pageNum);
        void setParent(IXFileHandle &ixfileHandle, int child, int parent);
        void newNonLeafPage(int left, const Attribute &attribute, const void * key, int right, void * page);
        int getPageFreeSpaceSize(const void * page);
        IX_SlotDirectoryHeader getPageHeader(const void * page);
        void setPageHeader(void * page, const IX_SlotDirectoryHeader& header);
        int getAttrSize(const Attribute &attribute, const void *key);
        Entry getEntry(const int i, const void * page);
        void setEntry(const int i, const Entry entry, void * page); // i starts at 0
        RID getKeyRid(int offset, const Attribute &attribute, void * key, const void * page);
};


class IXFileHandle {
    friend class IndexManager;
    public:

    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

    RC readHeader(void *data); // get index header
    RC setHeader(void *data);  // set index header

    RC readPage(PageNum pageNum, void *data);                           // Get a specific page
    RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
    RC appendPage(const void *data);                                    // Append a specific page
    unsigned getNumberOfPages();                                        // Get the number of pages in the file

	// Put the current counter values of associated PF FileHandles into variables
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

    private:
        FILE *_fd;
        // Private helper methods
        void setfd(FILE *fd);
        FILE *getfd();

};


class IX_ScanIterator {
    friend class IndexManager;
    private:

        // private field
        IXFileHandle *ixfhptr;
        Attribute attribute;
        void *lowKey;
        void *highKey;
        bool lowKeyInclusive;
        bool highKeyInclusive;
        void * page; // current page
        IndexManager * ixm;
        uint16_t curEntryNum;

        // private method
        RC scanInit(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive);
    public:

		// Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();
};

#endif
