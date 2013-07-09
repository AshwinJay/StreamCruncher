/*
 * StreamCruncher:  Copyright (c) 2006-2008, Ashwin Jayaprakash. All Rights Reserved.
 * Contact:         ashwin {dot} jayaprakash {at} gmail {dot} com
 * Web:             http://www.StreamCruncher.com
 * 
 * This file is part of StreamCruncher.
 * 
 *     StreamCruncher is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 * 
 *     StreamCruncher is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 * 
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with StreamCruncher. If not, see <http://www.gnu.org/licenses/>.
 */
package streamcruncher.innards.file;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import streamcruncher.api.artifact.RunningQuery;
import streamcruncher.api.artifact.TableSpec;
import streamcruncher.boot.Component;
import streamcruncher.boot.ConfigKeys;
import streamcruncher.boot.Registry;
import streamcruncher.innards.InnardsManager;
import streamcruncher.innards.core.stream.InStream;
import streamcruncher.innards.db.cache.SchedulableCachedData;
import streamcruncher.util.LoggerManager;

/*
 * Author: Ashwin Jayaprakash Date: Feb 25, 2006 Time: 10:14:40 AM
 */

public class FileManager implements Component {
    public static final String artificatsDirName = ".artifacts";

    public static final String instreamExtn = ".in";

    public static final String runningQueryExtn = ".rq";

    public static final String cachedDataExtn = ".cd";

    // --------------------

    protected File artificatsDir;

    // Used by the Parser.
    protected HashMap<String, TableSpec> inStreamTableSpecCache;

    // --------------------

    public void start(Object... params) throws Exception {
        Properties properties = (Properties) params[0];

        String mainStorage = properties.getProperty(ConfigKeys.Artifact.STORAGE_DIR);
        File mainDir = new File(mainStorage);
        checkDir(mainDir);

        artificatsDir = new File(mainDir, artificatsDirName);
        checkDir(artificatsDir);

        inStreamTableSpecCache = new HashMap<String, TableSpec>();

        // --------------------

        Logger logger = Registry.getImplFor(LoggerManager.class).getLogger(
                InnardsManager.class.getName());
        logger.log(Level.INFO, "Started");
    }

    protected void checkDir(File file) throws IOException, FileNotFoundException {
        if (file.exists() == false) {
            if (file.mkdirs() == false) {
                throw new FileNotFoundException("The directory: " + file.getAbsolutePath()
                        + " does not exist and neither could it be created.");
            }
        }
    }

    public void stop() throws Exception {
        artificatsDir = null;

        inStreamTableSpecCache.clear();
        inStreamTableSpecCache = null;

        // --------------------

        Logger logger = Registry.getImplFor(LoggerManager.class).getLogger(
                InnardsManager.class.getName());
        logger.log(Level.INFO, "Stopped");
    }

    // --------------------

    public TableSpec loadTableSpec(String schema, String name) throws FileManagerException {
        String key = getFQN(schema, name);
        TableSpec tableSpec = inStreamTableSpecCache.get(key);
        if (tableSpec == null) {
            throw new FileManagerException(
                    "There is no InStream/TableSpec registered under this name: " + key);
        }
        return tableSpec;
    }

    // --------------------

    public InStream[] loadInStreams() throws FileManagerException {
        InStream[] inStreams = {};
        LinkedList<InStream> list = new LinkedList<InStream>();

        try {
            File[] instreamFiles = artificatsDir.listFiles(new FileFilter() {
                public boolean accept(File pathname) {
                    return pathname.isFile() && pathname.getName().endsWith(instreamExtn);
                }
            });

            for (File file : instreamFiles) {
                ObjectInputStream in = new ObjectInputStream(new FileInputStream(file));
                InStream inStream = (InStream) in.readObject();
                in.close();

                // Do not add to Cache.
                list.add(inStream);
            }
        }
        catch (Exception e) {
            throw new FileManagerException(e);
        }

        // --------------------

        inStreams = list.toArray(inStreams);
        return inStreams;
    }

    public void saveInStream(InStream inStream, boolean overwrite) throws FileManagerException {
        String fqn = getFQN(inStream);
        String fileName = fqn + instreamExtn;
        File file = new File(artificatsDir, fileName);
        if (overwrite == false && file.exists()) {
            throw new FileManagerException("The File: " + fileName
                    + " in which the InStream is stored, already exists.");
        }

        try {
            ObjectOutput out = new ObjectOutputStream(new FileOutputStream(file));
            out.writeObject(inStream);
            out.close();

            // Create a dummy Spec for use by the Parser.
            TableSpec spec = new TableSpec(inStream.getSchema(), inStream.getName(), inStream
                    .getRowSpec(), null, null, false, true);
            inStreamTableSpecCache.put(fqn, spec);
        }
        catch (Exception e) {
            throw new FileManagerException(e);
        }
    }

    /**
     * @param inStream
     * @return <code>true</code> if delete succeeded.
     */
    public boolean deleteInStream(InStream inStream) {
        String key = getFQN(inStream);
        String fileName = key + instreamExtn;
        File file = new File(artificatsDir, fileName);
        boolean b = file.delete();

        inStreamTableSpecCache.remove(key);
        return b;
    }

    protected String getFQN(InStream inStream) {
        String key = (inStream.getSchema() != null) ? (inStream.getSchema() + "." + inStream
                .getName()) : inStream.getName();
        return key;
    }

    protected String getFQN(String schema, String name) {
        String key = (schema == null) ? name : (schema + "." + name);
        return key;
    }

    // --------------------

    public RunningQuery[] loadRunningQueries() throws FileManagerException {
        RunningQuery[] queries = {};
        LinkedList<RunningQuery> list = new LinkedList<RunningQuery>();

        try {
            File[] queryFiles = artificatsDir.listFiles(new FileFilter() {
                public boolean accept(File pathname) {
                    return pathname.isFile() && pathname.getName().endsWith(runningQueryExtn);
                }
            });
            for (File file : queryFiles) {
                ObjectInputStream in = new ObjectInputStream(new FileInputStream(file));
                RunningQuery query = (RunningQuery) in.readObject();
                in.close();

                list.add(query);
            }
        }
        catch (Exception e) {
            throw new FileManagerException(e);
        }

        // --------------------

        queries = list.toArray(queries);
        return queries;
    }

    public void saveRunningQuery(RunningQuery query, boolean overwrite) throws FileManagerException {
        String fileName = query.getName() + runningQueryExtn;
        File file = new File(artificatsDir, fileName);
        if (overwrite == false && file.exists()) {
            throw new FileManagerException("The File: " + fileName
                    + " in which the RunningQuery is stored, already exists.");
        }

        try {
            ObjectOutput out = new ObjectOutputStream(new FileOutputStream(file));
            out.writeObject(query);
            out.close();
        }
        catch (Exception e) {
            throw new FileManagerException(e);
        }
    }

    /**
     * @param query
     * @return <code>true</code> if delete succeeded.
     */
    public boolean deleteRunningQuery(RunningQuery query) {
        String fileName = query.getName() + runningQueryExtn;
        File file = new File(artificatsDir, fileName);
        return file.delete();
    }

    // --------------------

    /**
     * @return
     * @throws FileManagerException
     */
    public SchedulableCachedData[] loadCachedData() throws FileManagerException {
        SchedulableCachedData[] data = {};
        LinkedList<SchedulableCachedData> list = new LinkedList<SchedulableCachedData>();

        try {
            File[] cachedDataFiles = artificatsDir.listFiles(new FileFilter() {
                public boolean accept(File pathname) {
                    return pathname.isFile() && pathname.getName().endsWith(cachedDataExtn);
                }
            });
            for (File file : cachedDataFiles) {
                ObjectInputStream in = new ObjectInputStream(new FileInputStream(file));
                SchedulableCachedData cachedData = (SchedulableCachedData) in.readObject();
                in.close();

                list.add(cachedData);
            }
        }
        catch (Exception e) {
            throw new FileManagerException(e);
        }

        // --------------------

        data = list.toArray(data);
        return data;
    }

    /**
     * @param cachedData
     * @param overwrite
     * @throws FileManagerException
     */
    public void saveCachedData(SchedulableCachedData cachedData, boolean overwrite)
            throws FileManagerException {
        String fileName = Math.abs(cachedData.getSql().hashCode() * 37) + cachedDataExtn;
        File file = new File(artificatsDir, fileName);
        if (overwrite == false && file.exists()) {
            throw new FileManagerException("The File: " + fileName
                    + " in which the CachedData is stored, already exists.");
        }

        try {
            ObjectOutput out = new ObjectOutputStream(new FileOutputStream(file));
            out.writeObject(cachedData);
            out.close();
        }
        catch (Exception e) {
            throw new FileManagerException(e);
        }
    }

    /**
     * @param cachedData
     * @return <code>true</code> if delete succeeded.
     */
    public boolean deleteCachedData(SchedulableCachedData cachedData) {
        String fileName = Math.abs(cachedData.getSql().hashCode() * 37) + cachedDataExtn;
        File file = new File(artificatsDir, fileName);
        return file.delete();
    }
}
