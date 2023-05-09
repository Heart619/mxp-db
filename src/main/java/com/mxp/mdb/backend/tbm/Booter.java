package com.mxp.mdb.backend.tbm;

import com.mxp.mdb.backend.utils.Panic;
import com.mxp.mdb.backend.utils.Parser;
import com.mxp.mdb.common.error.Error;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

/**
 * 记录第一个表的uid
 *
 * @author mxp
 * @date 2023/4/19 9:51
 */
public class Booter {

    public static final String BOOTER_NAME = "boot.bt";
    public static final String BOOTER_TMP_NAME = "boot.bt.tmp";

    String path;
    File file;

    private Booter(String path, File file) {
        this.path = path;
        this.file = file;
    }

    public static Booter create(String path) {
        removeBadTmp(path);
        File f = new File(path + File.separator + BOOTER_NAME);
        try {
            if (!f.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        if (!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }
        Booter booter = new Booter(path, f);
        booter.update(Parser.longToByte(0));
        return booter;
    }

    public static Booter open(String path) {
        removeBadTmp(path);
        File f = new File(path + File.separator + BOOTER_NAME);
        if (!f.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        if (!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }
        return new Booter(path, f);
    }

    private static void removeBadTmp(String path) {
        new File(path + File.separator + BOOTER_TMP_NAME).delete();
    }

    public byte[] load() {
        byte[] buf = null;
        try {
            buf = Files.readAllBytes(file.toPath());
        } catch (IOException e) {
            Panic.panic(e);
        }
        return buf;
    }

    public void update(byte[] data) {
        File tmp = new File(path + File.separator + BOOTER_TMP_NAME);
        try {
            tmp.createNewFile();
        } catch (Exception e) {
            Panic.panic(e);
        }
        if (!tmp.canRead() || !tmp.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }
        try (FileOutputStream out = new FileOutputStream(tmp)) {
            out.write(data);
            out.flush();
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            Files.move(tmp.toPath(), new File(path + File.separator + BOOTER_NAME).toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            Panic.panic(e);
        }
        file = new File(path + File.separator + BOOTER_NAME);
        if (!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }
    }
}
