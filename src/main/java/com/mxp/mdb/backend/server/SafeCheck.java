package com.mxp.mdb.backend.server;

import com.mxp.mdb.backend.dm.DataManager;
import com.mxp.mdb.backend.dm.DataManagerImpl;
import com.mxp.mdb.backend.tbm.TableManager;
import com.mxp.mdb.backend.tbm.TableManagerImpl;
import com.mxp.mdb.backend.tm.MDBTransactionManager;
import com.mxp.mdb.backend.vm.VersionManager;
import com.mxp.mdb.backend.vm.VersionManagerImpl;

import java.io.File;

/**
 * @author mxp
 * @date 2023/4/20 17:30
 */
public class SafeCheck {

    private static final String path = "E:\\code\\M-DB";

    public static TableManager before() {
        File file = new File(path + File.separator + MDBTransactionManager.XID_NAME);
        if (file.exists()) {
            return open();
        }
        return create();
    }

    private static TableManager create() {
        MDBTransactionManager tm = MDBTransactionManager.create(path);
        DataManager dataManager = DataManagerImpl.create(path, 100000, tm);
        VersionManager versionManager = new VersionManagerImpl(tm, dataManager);
        return TableManagerImpl.create(path, versionManager, dataManager);
    }

    private static TableManager open() {
        MDBTransactionManager tm = MDBTransactionManager.open(path);
        DataManager dataManager = DataManagerImpl.open(path, 100000, tm);
        VersionManager versionManager = new VersionManagerImpl(tm, dataManager);
        return TableManagerImpl.open(path, versionManager, dataManager);
    }

    public static void safeClose(TableManager tableManager) {
        ((TableManagerImpl) tableManager).close();
    }
}
