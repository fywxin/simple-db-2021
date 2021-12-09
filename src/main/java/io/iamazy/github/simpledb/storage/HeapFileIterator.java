package io.iamazy.github.simpledb.storage;

import io.iamazy.github.simpledb.common.Database;
import io.iamazy.github.simpledb.common.DbException;
import io.iamazy.github.simpledb.common.Permissions;
import io.iamazy.github.simpledb.transaction.TransactionAbortedException;
import io.iamazy.github.simpledb.transaction.TransactionId;

import java.util.Iterator;

public class HeapFileIterator extends AbstractDbFileIterator {

    final TransactionId tid;
    final HeapFile f;
    Iterator<Tuple> it = null;
    HeapPage curPage = null;

    public HeapFileIterator(HeapFile f, TransactionId tid) {
        this.f = f;
        this.tid = tid;
    }

    @Override
    protected Tuple readNext() throws DbException, TransactionAbortedException {
        // 前一个 Page 的 Tuple 已遍历完，需要切换到下一 Page
        if (it != null && !it.hasNext()) {
            it = null;
        }
        while (it == null && curPage != null && f.numPages() > curPage.getId().getPageNumber() + 1) {
            HeapPageId pageId = new HeapPageId(f.getId(), curPage.getId().getPageNumber() + 1);
            HeapPage nextPage = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
            if (nextPage == null) {
                curPage = null;
            } else {
                curPage = nextPage;
                it = curPage.iterator();
                if (!it.hasNext()) {
                    it = null;
                }
            }
        }
        if (it == null) {
            return null;
        }
        return it.next();
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        HeapPageId pageId = new HeapPageId(f.getId(), 0);
        curPage = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
        it = curPage.iterator();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    @Override
    public void close() {
        super.close();
        it = null;
        curPage = null;
    }
}
