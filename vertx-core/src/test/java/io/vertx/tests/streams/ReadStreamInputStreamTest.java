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
import io.vertx.core.streams.impl.ReadStreamInputStream;
import io.vertx.test.core.VertxTestBase;
import io.vertx.test.fakestream.FakeStream;
import org.junit.Test;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ReadStreamInputStreamTest extends VertxTestBase {

  @Test
  public void testReadSingleChunk() throws Exception {
    FakeStream<Buffer> source = new FakeStream<>();
    try (ReadStreamInputStream is = new ReadStreamInputStream(source, 65536)) {
      source.write(Buffer.buffer("hello"));
      source.end();
      byte[] buf = new byte[5];
      int n = is.read(buf, 0, 5);
      assertEquals(5, n);
      assertEquals("hello", new String(buf, 0, n));
      assertEquals(-1, is.read(buf, 0, 5));
    }
  }

  @Test
  public void testReadSingleByte() throws Exception {
    FakeStream<Buffer> source = new FakeStream<>();
    try (ReadStreamInputStream is = new ReadStreamInputStream(source, 65536)) {
      source.write(Buffer.buffer(new byte[]{42}));
      source.end();
      assertEquals(42, is.read());
      assertEquals(-1, is.read());
    }
  }

  @Test
  public void testErrorPropagation() {
    FakeStream<Buffer> source = new FakeStream<>();
    try (ReadStreamInputStream is = new ReadStreamInputStream(source, 65536)) {
      source.fail(new RuntimeException("boom"));
      int ignored = is.read();
      fail();
    } catch (IOException e) {
      assertEquals("boom", e.getCause().getMessage());
    }
  }

  @Test
  public void testReadAfterClose() throws Exception {
    FakeStream<Buffer> source = new FakeStream<>();
    ReadStreamInputStream is = new ReadStreamInputStream(source, 65536);
    is.close();
    try {
      int ignored = is.read();
      fail();
    } catch (IOException expected) {
    }
  }

  @Test
  public void testAvailable() throws Exception {
    FakeStream<Buffer> source = new FakeStream<>();
    try (ReadStreamInputStream is = new ReadStreamInputStream(source, 65536)) {
      assertEquals(0, is.available());
      source.write(Buffer.buffer(new byte[100]));
      assertEquals(100, is.available());
    }
  }

  @Test
  public void testImmediateEnd() throws Exception {
    FakeStream<Buffer> source = new FakeStream<>();
    try (ReadStreamInputStream is = new ReadStreamInputStream(source, 65536)) {
      source.end();
      assertEquals(-1, is.read());
    }
  }

  @Test
  public void testEmptyBufferSkipped() throws Exception {
    FakeStream<Buffer> source = new FakeStream<>();
    try (ReadStreamInputStream is = new ReadStreamInputStream(source, 65536)) {
      source.write(Buffer.buffer(new byte[0]));
      source.write(Buffer.buffer("data"));
      source.end();
      byte[] buf = new byte[4];
      int n = is.read(buf, 0, 4);
      assertEquals(4, n);
      assertEquals("data", new String(buf, 0, n));
    }
  }

  @Test
  public void testBackPressurePause() throws Exception {
    FakeStream<Buffer> source = new FakeStream<>();
    try (ReadStreamInputStream ignored = new ReadStreamInputStream(source, 100)) {
      source.write(Buffer.buffer(new byte[120]));
      assertTrue(source.isPaused());
    }
  }

  @Test
  public void testBackPressureResume() throws Exception {
    FakeStream<Buffer> source = new FakeStream<>();
    try (ReadStreamInputStream is = new ReadStreamInputStream(source, 100)) {
      source.write(Buffer.buffer(new byte[120]));
      assertTrue(source.isPaused());
      byte[] buf = new byte[96];
      int n = is.read(buf, 0, 96);
      assertEquals(96, n);
      assertFalse(source.isPaused());
    }
  }

  @Test
  public void testCloseWakesBlockedReader() throws Exception {
    FakeStream<Buffer> source = new FakeStream<>();
    ReadStreamInputStream is = new ReadStreamInputStream(source, 65536);
    boolean[] exceptionCaught = new boolean[1];
    Thread reader = new Thread(() -> {
      try {
        int ignored = is.read();
      } catch (IOException expected) {
        exceptionCaught[0] = true;
      }
    });
    reader.start();
    assertWaitUntil(() -> reader.getState() == Thread.State.WAITING);
    is.close();
    reader.join(5000);
    assertFalse(reader.isAlive());
    assertTrue(exceptionCaught[0]);
  }

  @Test
  public void testBlockingRead() throws Exception {
    FakeStream<Buffer> source = new FakeStream<>();
    ReadStreamInputStream is = new ReadStreamInputStream(source, 65536);
    int[] result = new int[1];
    byte[] data = new byte[5];
    Thread reader = new Thread(() -> {
      try {
        result[0] = is.read(data, 0, 5);
      } catch (IOException e) {
        fail(e.getMessage());
      }
    });
    reader.start();
    assertWaitUntil(() -> reader.getState() == Thread.State.WAITING);
    source.write(Buffer.buffer("hello"));
    reader.join(5000);
    assertFalse(reader.isAlive());
    assertEquals(5, result[0]);
    assertEquals("hello", new String(data, 0, result[0]));
    is.close();
  }

  // --- Item 1: Read timeout ---

  @Test
  public void testReadTimeout() throws Exception {
    FakeStream<Buffer> source = new FakeStream<>();
    long start = System.nanoTime();
    try (ReadStreamInputStream is = new ReadStreamInputStream(source, 65536,
        200, TimeUnit.MILLISECONDS)) {
      int ignored = is.read();
      fail("Expected InterruptedIOException");
    } catch (InterruptedIOException e) {
      long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
      assertTrue("Timeout took " + elapsed + "ms", elapsed >= 150 && elapsed < 5000);
      assertTrue(e.getMessage().contains("timed out"));
    }
  }

  @Test
  public void testReadTimeoutBulk() throws Exception {
    FakeStream<Buffer> source = new FakeStream<>();
    try (ReadStreamInputStream is = new ReadStreamInputStream(source, 65536,
        200, TimeUnit.MILLISECONDS)) {
      byte[] buf = new byte[10];
      int ignored = is.read(buf, 0, 10);
      fail("Expected InterruptedIOException");
    } catch (InterruptedIOException e) {
      assertTrue(e.getMessage().contains("timed out"));
    }
  }

  @Test
  public void testNoTimeoutWhenDataAvailable() throws Exception {
    FakeStream<Buffer> source = new FakeStream<>();
    try (ReadStreamInputStream is = new ReadStreamInputStream(source, 65536,
        200, TimeUnit.MILLISECONDS)) {
      source.write(Buffer.buffer("hello"));
      source.end();
      byte[] buf = new byte[5];
      int n = is.read(buf, 0, 5);
      assertEquals(5, n);
      assertEquals("hello", new String(buf, 0, n));
    }
  }

  // --- Item 4: InterruptedIOException instead of sneaky-throw ---

  @Test
  public void testInterruptedReadThrowsInterruptedIOException() throws Exception {
    FakeStream<Buffer> source = new FakeStream<>();
    ReadStreamInputStream is = new ReadStreamInputStream(source, 65536);
    AtomicBoolean gotCorrectException = new AtomicBoolean(false);
    AtomicBoolean interruptFlagPreserved = new AtomicBoolean(false);
    Thread reader = new Thread(() -> {
      try {
        int ignored = is.read();
      } catch (InterruptedIOException e) {
        gotCorrectException.set(true);
        interruptFlagPreserved.set(Thread.currentThread().isInterrupted());
      } catch (IOException e) {
        // wrong exception type
      }
    });
    reader.start();
    assertWaitUntil(() -> reader.getState() == Thread.State.WAITING);
    reader.interrupt();
    reader.join(5000);
    assertFalse(reader.isAlive());
    assertTrue(gotCorrectException.get());
    assertTrue(interruptFlagPreserved.get());
    is.close();
  }

  // --- Item 3: Initial buffer prepend ---

  @Test
  public void testInitialData() throws Exception {
    FakeStream<Buffer> source = new FakeStream<>();
    Buffer initial = Buffer.buffer("prefix-");
    try (ReadStreamInputStream is = new ReadStreamInputStream(source, 65536, initial)) {
      source.write(Buffer.buffer("suffix"));
      source.end();
      byte[] buf = new byte[100];
      int n = is.read(buf, 0, 100);
      assertEquals(7, n);
      assertEquals("prefix-", new String(buf, 0, n));
      n = is.read(buf, 0, 100);
      assertEquals(6, n);
      assertEquals("suffix", new String(buf, 0, n));
      assertEquals(-1, is.read(buf, 0, 100));
    }
  }

  @Test
  public void testInitialDataNull() throws Exception {
    FakeStream<Buffer> source = new FakeStream<>();
    try (ReadStreamInputStream is = new ReadStreamInputStream(source, 65536,
        null)) {
      source.write(Buffer.buffer("data"));
      source.end();
      byte[] buf = new byte[4];
      assertEquals(4, is.read(buf, 0, 4));
      assertEquals("data", new String(buf, 0, 4));
    }
  }

  @Test
  public void testInitialDataEmpty() throws Exception {
    FakeStream<Buffer> source = new FakeStream<>();
    try (ReadStreamInputStream is = new ReadStreamInputStream(source, 65536,
        Buffer.buffer())) {
      source.write(Buffer.buffer("data"));
      source.end();
      byte[] buf = new byte[4];
      assertEquals(4, is.read(buf, 0, 4));
    }
  }

  @Test
  public void testInitialDataBackPressure() throws Exception {
    FakeStream<Buffer> source = new FakeStream<>();
    Buffer initial = Buffer.buffer(new byte[120]);
    try (ReadStreamInputStream ignored = new ReadStreamInputStream(source, 100, initial)) {
      assertTrue(source.isPaused());
    }
  }

  // --- Item 5: Close-before-end callback ---

  @Test
  public void testOnCloseBeforeEndCallback() throws Exception {
    FakeStream<Buffer> source = new FakeStream<>();
    AtomicBoolean callbackFired = new AtomicBoolean(false);
    ReadStreamInputStream is = new ReadStreamInputStream(source, 65536,
        null, 0, TimeUnit.MILLISECONDS, () -> callbackFired.set(true));
    source.write(Buffer.buffer("data"));
    is.close();
    assertTrue(callbackFired.get());
  }

  @Test
  public void testOnCloseAfterEndNoCallback() throws Exception {
    FakeStream<Buffer> source = new FakeStream<>();
    AtomicBoolean callbackFired = new AtomicBoolean(false);
    ReadStreamInputStream is = new ReadStreamInputStream(source, 65536,
        null, 0, TimeUnit.MILLISECONDS, () -> callbackFired.set(true));
    source.end();
    byte[] buf = new byte[10];
    assertEquals(-1, is.read(buf, 0, 10));
    is.close();
    assertFalse(callbackFired.get());
  }

  @Test
  public void testOnCloseBeforeEndNullCallback() throws Exception {
    FakeStream<Buffer> source = new FakeStream<>();
    ReadStreamInputStream is = new ReadStreamInputStream(source, 65536,
        null, 0, TimeUnit.MILLISECONDS, null);
    is.close();
  }
}
