package TM;

import common.Errors;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import utils.Panic;

/**
* The following exposes transaction manager API
* */
public interface TransactionManagerAPI {
    long begin(); // begin a transaction
    void commit(long xid); // commit a transaction
    void abort(long xid); // abort a transaction
    boolean isActive(long xid);
    boolean isCommitted(long xid);
    boolean isAborted(long xid);
    void close(); // close this transaction manager

    /**
     * Create a .xid file and set its header as 0
     */
    public static TransanctionManagerImpl create(String path) {
        File xidFile = new File(path + TransanctionManagerImpl.XID_SUFFIX);
        try {
            if (!xidFile.createNewFile()) {
                Panic.panic(Errors.FileExistsException);
            }
        } catch (IOException e) {
            Panic.panic(e);
        }

        // check if this file can be written or read
        if (!xidFile.canRead() || !xidFile.canWrite()) {
            Panic.panic(Errors.FileCannotRWException);
        }

        RandomAccessFile raf = null;
        FileChannel fc = null;
        try {
            raf = new RandomAccessFile(xidFile, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        // write the header
        ByteBuffer buf = ByteBuffer.wrap(new byte[TransanctionManagerImpl.LEN_XID_HEADER_LENGTH]);
        try {
            assert fc != null;
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        return new TransanctionManagerImpl(raf, fc);
    }

    /**
     * Open an existing xid file
     * @return a transactionManagerImpl object
     */
    public static TransanctionManagerImpl open(String path) {
        File xidFile = new File(path + TransanctionManagerImpl.XID_SUFFIX);
        if (!xidFile.exists()) {
            Panic.panic(Errors.FieldNotFoundException);
        }

        // check if this file can be written or read
        if (!xidFile.canRead() || !xidFile.canWrite()) {
            Panic.panic(Errors.FileCannotRWException);
        }

        RandomAccessFile raf = null;
        FileChannel fc = null;
        try {
            raf = new RandomAccessFile(xidFile, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        return new TransanctionManagerImpl(raf, fc);
    }

}
