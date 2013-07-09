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
package streamcruncher.util;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.LinkedHashMap;
import java.util.Map;

/*
 * Author: Ashwin Jayaprakash Date: Apr 9, 2006 Time: 5:09:51 PM
 */

public class SimpleJobBatchExecutionException extends Exception {
    private static final long serialVersionUID = 1L;

    protected final LinkedHashMap<String, Throwable> errors;

    /**
     * @param message
     */
    public SimpleJobBatchExecutionException(String message) {
        super(message);

        this.errors = new LinkedHashMap<String, Throwable>();
    }

    public SimpleJobBatchExecutionException(String message, Throwable cause) {
        super(message, cause);

        this.errors = new LinkedHashMap<String, Throwable>();
    }

    public SimpleJobBatchExecutionException(Throwable cause) {
        super(cause);

        this.errors = new LinkedHashMap<String, Throwable>();
    }

    /**
     * @return Returns the exceptions.
     */
    public Map<String, Throwable> getErrors() {
        return errors;
    }

    // -----------------

    /**
     * {@inheritDoc}
     */
    @Override
    public String getMessage() {
        return super.getMessage() + " [" + errors.size() + " Exceptions]";
    }

    public void addError(String jobName, Throwable throwable) {
        errors.put(jobName, throwable);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void printStackTrace(PrintStream stream) {
        super.printStackTrace(stream);

        stream.println(" -- Exceptions -- ");
        for (String name : errors.keySet()) {
            Throwable t = errors.get(name);
            stream.println(" [" + name + "]");
            t.printStackTrace(stream);
            stream.println();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void printStackTrace(PrintWriter writer) {
        super.printStackTrace(writer);

        writer.println(" -- Exceptions -- ");
        for (String name : errors.keySet()) {
            Throwable t = errors.get(name);
            writer.println(" [" + name + "]");
            t.printStackTrace(writer);
            writer.println();
        }
    }
}
