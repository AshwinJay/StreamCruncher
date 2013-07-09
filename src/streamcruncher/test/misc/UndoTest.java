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
package streamcruncher.test.misc;

import java.util.LinkedList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import streamcruncher.test.TestGroupNames;
import streamcruncher.util.undo.Helper;
import streamcruncher.util.undo.Helper.UndoRunner;

/*
 * Author: Ashwin Jayaprakash Date: Jul 30, 2006 Time: 10:52:28 AM
 */

public class UndoTest {
    public void do1() {
        System.out.println("do1");
    }

    public void undo1() {
        System.out.println("undo1");
    }

    public void do2(List<Number> list) {
        System.out.println("do2(List<Number>)");
    }

    public void undo2(Integer a) {
        System.out.println("undo2(Integer)");
    }

    public void undo2(List<Number> list) {
        System.out.println("undo2(List<Number>)");
    }

    public void do2(LinkedList<Integer> list) {
        System.out.println("do2(LinkedList<Number>)");
    }

    public void undo2(LinkedList<Integer> list) {
        System.out.println("undo2(LinkedList<Number>)");
    }

    public void do3() {
        System.out.println("do3");
    }

    public void undo3() {
        throw new RuntimeException("Exception in undo3");
    }

    public void do4() {
        System.out.println("do4");
    }

    public void undo4(LinkedList<Integer> list) {
        System.out.println("undo4(LinkedList<Number>)");
    }

    // ---------

    @Test(dependsOnGroups = { TestGroupNames.SC_INIT_REQUIRED }, groups = { TestGroupNames.SC_TEST })
    public void test() {
        Helper helper = new Helper();

        do1();
        helper.registerUndoEntry(new Helper.UndoRunner() {
            public void undo() throws Exception {
                UndoTest.this.undo1();
            }
        });

        final LinkedList<Integer> list = new LinkedList<Integer>();
        do2(list);
        helper.registerUndoEntry(new Helper.UndoRunner() {
            public void undo() throws Exception {
                UndoTest.this.undo2(list);
            }
        });

        do3();
        helper.registerUndoEntry(new Helper.UndoRunner() {
            public void undo() throws Exception {
                UndoTest.this.undo3();
            }
        });

        do4();
        helper.registerUndoEntry(new Helper.UndoRunner() {
            public void undo() throws Exception {
                UndoTest.this.undo4(list);
            }
        });

        System.out.println("-Remaining-");

        List<UndoRunner> remainingEntries = helper.undo(true);
        Assert.assertEquals(remainingEntries.size(), 2);
        for (UndoRunner runner : remainingEntries) {
            System.out.println(runner);
        }
    }
}
