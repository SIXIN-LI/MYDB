package TM;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import utils.Panic;
import common.Errors;
import utils.Parser;

public class TransanctionManagerImpl implements TransactionManagerAPI{

    // XID file header length
    static final int LEN_XID_HEADER_LENGTH = 8;
    // the size for each transaction xid
    private static final int XID_FIELD_SIZE = 1;
    // three states of transactions
    private static final byte FIELD_TRAN_ACTIVE   = 0;
    private static final byte FIELD_TRAN_COMMITTED = 1;
    private static final byte FIELD_TRAN_ABORTED  = 2;
    // super transaction which is always committed
    public static final long SUPER_XID = 0;
    // XID file suffix
    static final String XID_SUFFIX = ".xid";

    private RandomAccessFile file;
    private FileChannel fc;
    private long xidCounter; // when do we use xid Counter
    private Lock counterLock;

    /**
    * This constructor opens a file channel for read and write access of XID file
    * */
    public TransanctionManagerImpl(RandomAccessFile file, FileChannel fc) {
        this.file = file;
        this.fc = fc;
        this.counterLock = new ReentrantLock();
        checkXIDCounter();
    }

    /**
     * Validating the xid file by comparing its theoretical length based on xid file header
     * with the file's actual length
     */
    private void checkXIDCounter() {
        long fileLen = 0;
        try {
            fileLen = file.length();
        } catch (IOException e) {
            Panic.panic(Errors.BadXIDFileException);
        }
        if(fileLen < LEN_XID_HEADER_LENGTH) {
            Panic.panic(Errors.BadXIDFileException);
        }

        ByteBuffer buf = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);
        try {
            fc.position(0);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(Errors.BadXIDFileException);
        }
        this.xidCounter = Parser.parseLong(buf.array());
        long end = getXidPosition(this.xidCounter + 1);
        if(end != fileLen) {
            Panic.panic(Errors.BadXIDFileException);
        }
    }

    /**
     * Get transaction id based on their xid
     */
    private long getXidPosition(long xid) {
        return LEN_XID_HEADER_LENGTH + (xid-1) * XID_FIELD_SIZE;
    }

    /**
     * Begin a transaction by
     *  incrementing the xid counter,
     *  update this transaction to active
     *
     * @return transaction xid
     */
    @Override
    public long begin() {
        counterLock.lock();
        try {
            long xid = this.xidCounter + 1;
            updateTransactionState(xid, FIELD_TRAN_ACTIVE);
            incrXIDCounter();
            return xid;
        } finally {
            counterLock.unlock();
        }
    }

    @Override
    public void commit(long xid) {
        counterLock.lock();
        try {
            updateTransactionState(xid, FIELD_TRAN_COMMITTED);
        } finally {
            counterLock.unlock();
        }
    }

    @Override
    public void abort(long xid) {
        counterLock.lock();
        try {
            updateTransactionState(xid, FIELD_TRAN_ABORTED);
        } finally {
            counterLock.unlock();
        }
    }

    /**
     * Increment xid counter by 1, update the xid file header
     * and force the content to be written in the file
     */
    private void incrXIDCounter() {
        this.xidCounter++;
        ByteBuffer bf = ByteBuffer.wrap(Parser.long2Byte(this.xidCounter));
        try {
            fc.position(0);
            fc.write(bf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        // force the content in the channel to be written into the file
        try {
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * Find the offset based on xid and write state there
     * @param xid
     * @param state - three states of transaction
     */
    private void updateTransactionState(long xid, byte state) {
        long offset = getXidPosition(xid);
        byte[] tmp = new byte[XID_FIELD_SIZE];
        tmp[0] = state;
        ByteBuffer buf = ByteBuffer.wrap(tmp);

        try {
            fc.position(offset);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        try {
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    @Override
    public boolean isActive(long xid) {
        if (xid == SUPER_XID) return false;
        return checkXIDState(xid, FIELD_TRAN_ACTIVE);
    }

    @Override
    public boolean isCommitted(long xid) {
        if (xid == SUPER_XID) return true;
        return checkXIDState(xid, FIELD_TRAN_COMMITTED);
    }

    @Override
    public boolean isAborted(long xid) {
        if (xid == SUPER_XID) return false;
        return checkXIDState(xid, FIELD_TRAN_ABORTED);
    }

    private boolean checkXIDState(long xid, byte state) {
        long offset = getXidPosition(xid);
        ByteBuffer buf = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);

        try {
            fc.position(offset);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        return buf.array()[0] == state;
    }

    /**
     * Closing the file channel and the file
     */
    @Override
    public void close() {
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }
}
