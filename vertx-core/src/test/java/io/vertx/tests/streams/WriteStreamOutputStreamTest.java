/*
 * Copyright (c) 2011-2026 Contributors to the Eclipse Foundation
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
 * which is available at https://www.apache.org/licenses/LICENSE-2.0.
 *
 * SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
 */
package io.vertx.tests.streams;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.WriteStreamOutputStream;
import io.vertx.test.core.VertxTestBase;
import io.vertx.test.fakestream.FakeStream;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class WriteStreamOutputStreamTest extends VertxTestBase {

  @Test
  public void testSmallWriteAndFlush() throws Exception {
    FakeStream<Buffer> sink = new FakeStream<>();
    List<Buffer> written = new ArrayList<>();
    sink.handler(written::add);
    try (WriteStreamOutputStream os = new WriteStreamOutputStream(sink, 8192)) {
      os.write("hello".getBytes());
      assertEquals(0, written.size());  // accumulated, not flushed
      os.flush();
      assertEquals(1, written.size());
      assertEquals("hello", written.get(0).toString());
    }
  }

  @Test
  public void testLargeWriteDirect() throws Exception {
    FakeStream<Buffer> sink = new FakeStream<>();
    List<Buffer> written = new ArrayList<>();
    sink.handler(written::add);
    try (WriteStreamOutputStream os = new WriteStreamOutputStream(sink, 100)) {
      byte[] large = new byte[200];
      Arrays.fill(large, (byte) 'A');
      os.write(large);
      assertEquals(1, written.size());
      assertEquals(200, written.get(0).length());
    }
  }

  @Test
  public void testLargeWriteFlushesAccumulatorFirst() throws Exception {
    FakeStream<Buffer> sink = new FakeStream<>();
    List<Buffer> written = new ArrayList<>();
    sink.handler(written::add);
    try (WriteStreamOutputStream os = new WriteStreamOutputStream(sink, 100)) {
      os.write("small".getBytes());  // goes to accumulator
      byte[] large = new byte[200];
      os.write(large);  // should flush "small" first, then write large
      assertEquals(2, written.size());
      assertEquals("small", written.get(0).toString());
      assertEquals(200, written.get(1).length());
    }
  }

  @Test
  public void testSingleByteWrite() throws Exception {
    FakeStream<Buffer> sink = new FakeStream<>();
    List<Buffer> written = new ArrayList<>();
    sink.handler(written::add);
    try (WriteStreamOutputStream os = new WriteStreamOutputStream(sink, 8192)) {
      os.write(65); // 'A'
      os.flush();
      assertEquals(1, written.size());
      assertEquals("A", written.get(0).toString());
    }
  }

  @Test
  public void testCloseFlushesAndEnds() throws Exception {
    FakeStream<Buffer> sink = new FakeStream<>();
    List<Buffer> written = new ArrayList<>();
    sink.handler(written::add);
    WriteStreamOutputStream os = new WriteStreamOutputStream(sink, 8192);
    os.write("tail".getBytes());
    os.close();
    assertEquals(1, written.size());
    assertEquals("tail", written.get(0).toString());
    assertTrue(sink.isEnded());
  }

  @Test
  public void testWriteAfterClose() throws Exception {
    FakeStream<Buffer> sink = new FakeStream<>();
    WriteStreamOutputStream os = new WriteStreamOutputStream(sink, 8192);
    os.close();
    try {
      os.write(1);
      fail();
    } catch (IOException expected) {
    }
  }

  @Test
  public void testFlushEmpty() throws Exception {
    FakeStream<Buffer> sink = new FakeStream<>();
    List<Buffer> written = new ArrayList<>();
    sink.handler(written::add);
    try (WriteStreamOutputStream os = new WriteStreamOutputStream(sink, 8192)) {
      os.flush();
      assertEquals(0, written.size());
    }
  }

  @Test
  public void testSinkErrorSurfacesOnNextWrite() throws Exception {
    FakeStream<Buffer> sink = new FakeStream<>();
    AtomicBoolean shouldFail = new AtomicBoolean(false);
    sink.handler(b -> {
      if (shouldFail.get()) {
        throw new RuntimeException("sink broke");
      }
    });
    try (WriteStreamOutputStream os = new WriteStreamOutputStream(sink, 8192)) {
      os.write("first".getBytes());
      os.flush();
      shouldFail.set(true);
      os.write("second".getBytes());
      try {
        os.flush();
        fail("Expected IOException");
      } catch (IOException e) {
        assertTrue(e.getCause().getMessage().contains("sink broke"));
      }
    }
  }

  @Test
  public void testBackPressureBlocking() throws Exception {
    FakeStream<Buffer> sink = new FakeStream<>();
    sink.pause();
    sink.setWriteQueueMaxSize(0);
    sink.handler(b -> {});
    WriteStreamOutputStream os = new WriteStreamOutputStream(sink, 100);
    byte[] large = new byte[200];

    Thread writer = new Thread(() -> {
      try {
        os.write(large);
      } catch (IOException e) {
        fail(e.getMessage());
      }
    });
    writer.start();
    assertWaitUntil(() -> writer.getState() == Thread.State.WAITING);
    sink.resume();
    writer.join(5000);
    assertFalse(writer.isAlive());
  }
}
