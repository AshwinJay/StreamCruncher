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
package streamcruncher.boot;

/*
 * Author: Ashwin Jayaprakash Date: Dec 31, 2005 Time: 9:25:26 PM
 */

public interface ConfigKeys {
    public interface Logger {
        public static final String CONFIG_FILE_PATH = "log.config.file.path";
    }

    public interface Custom {
        public static final String CUSTOM_STORE_NAME = "custom.storename";
    }

    public interface DB {
        public static final String NAME = "db.name";

        public static final String PRESERVES_ARTIFACTS_ON_SHUTDOWN = "db.preservesartifacts.onshutdown";

        public static final String DRIVER_CLASS_NAME = "db.driver.classname";

        public static final String DRIVER_URL = "db.driver.url";

        public static final String USER = "db.user";

        public static final String PASSWORD = "db.password";

        public static final String SCHEMA = "db.schema";

        public static final String CONNECTION_POOL_MAX_SIZE = "db.connectionpool.maxsize";

        public static final String PRIVATE_VOLATILE_INSTANCE = "db.privatevolatile.instance";
    }

    public interface Artifact {
        public static final String STORAGE_DIR = "artifact.storage.dir";
    }

    public interface InStreamEventProcessor {
        public static final String PROCESSOR_THREADS_NUM = "instreameventprocessor.threads.num";

        public static final String PROCESSOR_THREAD_EMPTY_RUNS_BEFORE_PAUSE = "instreameventprocessor.thread.empty-runs-before-pause";

        public static final String PROCESSOR_THREAD_PAUSE_MSECS = "instreameventprocessor.thread.pause.msecs";
    }

    public interface ResultSetCacheRefresh {
        public static final String THREADS_NUM = "cacherefresh.threads.num";
    }

    public interface QueryScheduler {
        public static final String THREADS_NUM = "queryscheduler.threads.num";
    }

    public interface QueryRunner {
        public static final String THREADS_NUM = "queryrunner.threads.num";
    }

    public interface RowDisposer {
        public static final String DISPOSER_THREAD_PAUSE_MSECS = "rowdisposer.thread.pause.msecs";
    }

    public interface JobExecution {
        public static final String THREADS_NUM = "jobexecution.threads.num";
    }
}
