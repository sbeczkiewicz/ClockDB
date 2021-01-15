/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

/*
 * CS 564,GROUP 19
 * YANPENG ZHAO
 * YUHAN DAI
 * STEVEN BECZKIEWICZ
 */

namespace badgerdb {

    BufMgr::BufMgr(std::uint32_t bufs)
            : numBufs(bufs) {
        bufDescTable = new BufDesc[bufs];

        for (FrameId i = 0; i < bufs; i++) {
            bufDescTable[i].frameNo = i;
            bufDescTable[i].valid = false;
        }

        bufPool = new Page[bufs];

        int htsize = ((((int) (bufs * 1.2)) * 2) / 2) + 1;
        hashTable = new BufHashTbl(htsize);  // allocate the buffer hash table

        clockHand = bufs - 1;
    }

/**
 * deconstructor of buffer, delete bufpool, desctable and hashtable
 */

   BufMgr::~BufMgr() {
      // loops through all pages and flushes out dirty pages
      for(FrameId i = 0; i < numBufs; i++) {
         if(bufDescTable[i].valid && bufDescTable[i].dirty) {
            bufDescTable[i].file->writePage(bufPool[i]);
         }
      }
    // deallocates the buffer pool and the BufDesc table
    delete[] bufPool;
    delete[] bufDescTable;
    delete hashTable;
}


/**
 * increase clockhand by 1
 */
    void BufMgr::advanceClock() {
        clockHand = (clockHand + 1) % numBufs;
    }

/**
 * allocate a frame, iterate all  the  buffer to find right place
 * @param frame
 */
    void BufMgr::allocBuf(FrameId &frame) {

        uint32_t count = 0;
        while (count <= numBufs) {
            advanceClock();
            // increase hand by 1 every turn
            if (bufDescTable[clockHand].valid == 0) {
                frame = clockHand;
                return;
            }
            if (bufDescTable[clockHand].pinCnt == 0) {
                if (bufDescTable[clockHand].dirty) {
                    //save dirty file
                    bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
                }
                //remove file, we find the right place
                hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
                bufDescTable[clockHand].Clear();
                frame = clockHand;
                break;
            }
            if (bufDescTable[clockHand].refbit) {
                bufDescTable[clockHand].refbit = 0;
                continue;
            }

            count++;
        }
        if (count > numBufs) {
            //exceeding
            throw BufferExceededException();
        }


    }

/**
 * read the page, increase the count in already exist in buffer, else, allocate  the page
 * @param file
 * @param pageNo
 * @param page
 */
    void BufMgr::readPage(File *file, const PageId pageNo, Page *&page) {
        FrameId frame;
        try {
            //in case in the table
            hashTable->lookup(file, pageNo, frame);
            bufDescTable[frame].refbit = true;
            bufDescTable[frame].pinCnt++;
        } catch (const HashNotFoundException &e) {
            //not in the table
            allocBuf(frame);
            bufPool[frame] = file->readPage(pageNo);
            bufDescTable[frame].Set(file, pageNo);
            hashTable->insert(file, pageNo, frame);
        }
        //read the page
        page = &bufPool[frame];
    }

/**
 * unpin a page.
 * @param file
 * @param pageNo
 * @param dirty
 */
    void BufMgr::unPinPage(File *file, const PageId pageNo, const bool dirty) {
        FrameId frame;
        try {
            //if  already pinned
            hashTable->lookup(file, pageNo, frame);
            if (bufDescTable[frame].pinCnt) {
                //set dirty
                if (dirty) {
                    bufDescTable[frame].dirty = dirty;
                }
                //unpinning
                bufDescTable[frame].pinCnt--;
            } else {
                //not pinned, exception
                throw PageNotPinnedException(file->filename(), pageNo, frame);
            }
        }
        catch (const HashNotFoundException &e) {
        }
    }

/**
 * allocate a new page in file.
 * @param file
 * @param pageNo
 * @param page
 */
    void BufMgr::allocPage(File *file, PageId &pageNo, Page *&page) {
        //create new page
        Page i;
        //assign  page
        i = file->allocatePage();
        pageNo = i.page_number();
        //set pointer
        page = &i;
        //add pincnt
        readPage(file, pageNo, page);
    }

/**
 * save  file that is dirty to disk.
 * @param file
 */
    void BufMgr::flushFile(const File *file) {
        //iterate all file
        for (unsigned int i = 0; i < numBufs; i++) {
            if (bufDescTable[i].file == file) {
                //in case doesnt need flush
                if (bufDescTable[i].pinCnt > 0) {
                    throw PagePinnedException(file->filename(), bufDescTable[i].pageNo, bufDescTable[i].frameNo);
                }
                if (!bufDescTable[i].valid) {
                    throw BadBufferException(bufDescTable[i].frameNo, bufDescTable[i].dirty, bufDescTable[i].valid,
                                             bufDescTable[i].refbit);
                }
                //save file process, new page created.
                if (bufDescTable[i].dirty) {
                    bufDescTable[i].file->writePage(bufPool[i]);
                    bufDescTable[i].dirty = false;
                }
                //removing..
                hashTable->remove(bufDescTable[i].file, bufDescTable[i].pageNo);
                bufDescTable[i].Clear();

            }
        }
    }

/**
 * delete page  from file and from buffer pool
 * @param file
 * @param PageNo
 */
    void BufMgr::disposePage(File *file, const PageId PageNo) {
        FrameId frameNum;
        //delete the page
        try {
            hashTable->lookup(file, PageNo, frameNum);
            hashTable->remove(file, PageNo);
            bufDescTable[frameNum].Clear();
        } catch (const HashNotFoundException &e) {}
        //in case file not found
        file->deletePage(PageNo);
    }

    void BufMgr::printSelf(void) {
        BufDesc *tmpbuf;
        int validFrames = 0;

        for (std::uint32_t i = 0; i < numBufs; i++) {
            tmpbuf = &(bufDescTable[i]);
            std::cout << "FrameNo:" << i << " ";
            tmpbuf->Print();

            if (tmpbuf->valid == true)
                validFrames++;
        }

        std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
    }

}

