package io.iamazy.github.simpledb.storage;

import io.iamazy.github.simpledb.common.Database;
import io.iamazy.github.simpledb.common.DbException;
import io.iamazy.github.simpledb.common.Debug;
import io.iamazy.github.simpledb.common.Permissions;
import io.iamazy.github.simpledb.transaction.TransactionAbortedException;
import io.iamazy.github.simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private final File file;
    private final int tableId;
    private final TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.tableId = file.getAbsoluteFile().hashCode();
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return tableId;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(file, "r");
            raf.seek((long) pid.getPageNumber() * BufferPool.getPageSize());
            byte[] bytes = new byte[BufferPool.getPageSize()];
            if (raf.read(bytes) > 0) {
                HeapPageId pageId = new HeapPageId(tableId, pid.getPageNumber());
                return new HeapPage(pageId, bytes);
            }
        } catch (IOException e) {
            Debug.log(e.getMessage());
        } finally {
            try {
                if (raf != null) {
                    raf.close();
                }
            } catch (IOException e) {
                Debug.log(e.getMessage());
            }
        }
        throw new IllegalArgumentException("page not found");
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) (file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        Database.getBufferPool().getPage(tid, )
        return null;
    }

}

