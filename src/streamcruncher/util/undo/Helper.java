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
package streamcruncher.util.undo;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import streamcruncher.boot.Registry;
import streamcruncher.util.LoggerManager;

/*
 * Author: Ashwin Jayaprakash Date: Jul 30, 2006 Time: 9:37:27 AM
 */

public class Helper {
    protected final List<UndoRunner> undoItems;

    public Helper() {
        this.undoItems = new LinkedList<UndoRunner>();
    }

    public void registerUndoEntry(UndoRunner runner) {
        undoItems.add(runner);
    }

    /**
     * @param continueAfterException
     * @return The list of entries still which were not invoked after & during
     *         the Exception.
     */
    public List<UndoRunner> undo(boolean continueAfterException) {
        Collections.reverse(undoItems);

        Logger logger = Registry.getImplFor(LoggerManager.class).getLogger(Helper.class.getName());
        logger.log(Level.WARNING, "Starting 'Undo' action due to error.");

        for (Iterator<UndoRunner> iter = undoItems.iterator(); iter.hasNext();) {
            UndoRunner runner = iter.next();

            try {
                runner.undo();
                iter.remove();
            }
            catch (Throwable t) {
                logger.log(Level.SEVERE, "Undo invocation caused an error", t);

                if (continueAfterException == false) {
                    break;
                }
            }
        }

        return undoItems;
    }

    // -----------

    public interface UndoRunner {
        public void undo() throws Exception;
    }
}
