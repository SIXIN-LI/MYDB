package TM;

/**
* The following exposes transaction manager API
* */
public interface TransactionManagerAPI {
    // begin a transaction
    long begin();

    // commit a transaction
    void commit(long xid);

    // abort a transaction
    void abort(long xid);

    boolean isActive(long xid);
    boolean isCommitted(long xid);
    boolean isAborted(long xid);

    // close this transaction manager
    void close();

}
