package org.example.Tests;

import org.example.LSMTree;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class LSMTreeTest {

    @Test
    void getValueOf() throws IOException {
        LSMTree tree = new LSMTree(3,3,5, 10, true);
        tree.setValueOf("key", "value");
        assertEquals("value", tree.getValueOf("key"));
    }
    @Test
    void getValueFromSegment() throws IOException {
        LSMTree tree = new LSMTree(404,3,5, 10, true);
        for (int i = 0; i < 100; i++) {
            tree.setValueOf("key"+i, "value"+i);
        }
        assertEquals("value1", tree.getValueOf("key1"));
        assertEquals("value8", tree.getValueOf("key8"));
        assertEquals("value77", tree.getValueOf("key77"));
        assertEquals("value80", tree.getValueOf("key80"));
    }
    @Test
    void getValueOfOverWriteKey() throws IOException {
        LSMTree tree = new LSMTree(405,3,5, 10, true);
        for (int i = 0; i < 20; i++) {
            tree.setValueOf("key"+i, "value"+i);
        }
        tree.setValueOf("key8", "value8 New");
        tree.setValueOf("key1", "value1 New");

        for (int i = 20; i < 40; i++) {
            tree.setValueOf("key"+i, "value"+i);
        }
        tree.setValueOf("key1", "value1 Newest");
        assertEquals("value1 Newest", tree.getValueOf("key1"));
        assertEquals("value8 New", tree.getValueOf("key8"));
        assertNull(tree.getValueOf("key77"));
        assertEquals("value30", tree.getValueOf("key30"));
    }
    @Test
    void bloomFilter() throws IOException {
        LSMTree tree = new LSMTree(407,3,5, 10, true);
        for (int i = 0; i < 100; i++) {
            tree.setValueOf("key"+i, "value"+i);
        }
        for (int i = 0; i < 100; i++) {
            assertEquals("value"+i, tree.getValueOf("key"+i));
        }
    }
}