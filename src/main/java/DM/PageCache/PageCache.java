package DM.PageCache;

import DM.Page.Page;

/**
 * Release cache for the page
 */
public interface PageCache {
    int newPage(byte[] initData);
    Page getPage(int pgno) throws Exception;
    void close();
    void release(Page page);

    void truncateByPgno(int maxPgno);
    int getPageNumber();
    void flushPage(Page pg);
}
