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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class WriteStreamOutputStreamConcurrencyTest extends VertxTestBase {

  @Test
  public void testDataIntegrityManySmallWrites() throws Exception {
    FakeStream<Buffer> sink = new FakeStream<>();
    List<Buffer> written = new ArrayList<>();
    sink.handler(written::add);
    try (WriteStreamOutputStream os = new WriteStreamOutputStream(sink, 8192)) {
      for (int i = 0; i < 50000; i++) {
        os.write(i % 256);
      }
      os.flush();
    }

    int totalLength = 0;
    for (Buffer buf : written) {
      totalLength += buf.length();
    }
    assertEquals(50000, totalLength);

    byte[] combined = new byte[totalLength];
    int offset = 0;
    for (Buffer buf : written) {
      for (int j = 0; j < buf.length(); j++) {
        combined[offset++] = buf.getByte(j);
      }
    }

    for (int i = 0; i < 50000; i++) {
      assertEquals("Mismatch at index " + i, (byte)(i % 256), combined[i]);
    }
  }

  @Test
  public void testRapidFlushCycles() throws Exception {
    FakeStream<Buffer> sink = new FakeStream<>();
    List<Buffer> written = new ArrayList<>();
    sink.handler(written::add);
    try (WriteStreamOutputStream os = new WriteStreamOutputStream(sink, 8192)) {
      for (int i = 0; i < 10000; i++) {
        os.write("x".getBytes());
        os.flush();
      }
    }

    assertEquals(10000, written.size());
    for (int i = 0; i < written.size(); i++) {
      Buffer buf = written.get(i);
      assertEquals("Buffer " + i + " should have length 1", 1, buf.length());
      assertEquals("Buffer " + i + " should contain 'x'", (byte)'x', buf.getByte(0));
    }
  }

  @Test
  public void testCloseFlushesAllPendingData() throws Exception {
    FakeStream<Buffer> sink = new FakeStream<>();
    List<Buffer> written = new ArrayList<>();
    sink.handler(written::add);
    WriteStreamOutputStream os = new WriteStreamOutputStream(sink, 8192);

    for (int i = 0; i < 100; i++) {
      os.write(i);
    }
    os.close();

    int totalLength = 0;
    for (Buffer buf : written) {
      totalLength += buf.length();
    }
    assertEquals(100, totalLength);

    byte[] combined = new byte[totalLength];
    int offset = 0;
    for (Buffer buf : written) {
      for (int j = 0; j < buf.length(); j++) {
        combined[offset++] = buf.getByte(j);
      }
    }

    for (int i = 0; i < 100; i++) {
      assertEquals((byte)i, combined[i]);
    }

    assertTrue(sink.isEnded());
  }

  @Test
  public void testBackPressureThroughputWithDrain() throws Exception {
    FakeStream<Buffer> sink = new FakeStream<>();
    sink.pause();
    sink.setWriteQueueMaxSize(0);
    sink.handler(b -> {});

    WriteStreamOutputStream os = new WriteStreamOutputStream(sink, 100);

    Thread writer = new Thread(() -> {
      try {
        byte[] data = new byte[1024];
        for (int i = 0; i < data.length; i++) {
          data[i] = (byte)(i % 256);
        }
        os.write(data, 0, 1024);
      } catch (IOException e) {
        fail(e.getMessage());
      }
    });

    writer.start();
    assertWaitUntil(() -> writer.getState() == Thread.State.WAITING);

    sink.resume();

    writer.join(5000);
    assertFalse("Writer thread should have completed", writer.isAlive());
  }

  @Test
  public void testCloseWhileBackPressured() throws Exception {
    FakeStream<Buffer> sink = new FakeStream<>();
    sink.pause();
    sink.setWriteQueueMaxSize(0);
    sink.handler(b -> {});

    WriteStreamOutputStream os = new WriteStreamOutputStream(sink, 100);
    CountDownLatch writerDone = new CountDownLatch(1);

    Thread writer = new Thread(() -> {
      try {
        os.write(new byte[1024], 0, 1024);
      } catch (IOException ignored) {
      } finally {
        writerDone.countDown();
      }
    });

    writer.start();
    assertWaitUntil(() -> writer.getState() == Thread.State.WAITING);

    os.close();

    assertTrue("Writer must unblock within 5s", writerDone.await(5, TimeUnit.SECONDS));
  }

  @Test
  public void testDoubleCloseConcurrent() throws Exception {
    FakeStream<Buffer> sink = new FakeStream<>();
    sink.handler(b -> {});
    WriteStreamOutputStream os = new WriteStreamOutputStream(sink, 8192);

    os.write("data".getBytes());

    AtomicInteger endCount = new AtomicInteger();
    sink.endHandler(v -> endCount.incrementAndGet());

    int threads = 10;
    CountDownLatch startGate = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(threads);

    for (int i = 0; i < threads; i++) {
      new Thread(() -> {
        try {
          startGate.await();
          os.close();
        } catch (Exception ignored) {
        } finally {
          done.countDown();
        }
      }).start();
    }

    startGate.countDown();
    assertTrue("All threads must finish within 5s", done.await(5, TimeUnit.SECONDS));
    assertEquals("sink.end() must be called exactly once", 1, endCount.get());
  }

  @Test
  public void testErrorDuringBackPressureWait() throws Exception {
    FakeStream<Buffer> sink = new FakeStream<>();
    sink.pause();
    sink.setWriteQueueMaxSize(0);
    sink.handler(b -> {});

    WriteStreamOutputStream os = new WriteStreamOutputStream(sink, 100);
    CountDownLatch writerDone = new CountDownLatch(1);
    AtomicReference<Throwable> writerError = new AtomicReference<>();

    Thread writer = new Thread(() -> {
      try {
        os.write(new byte[1024], 0, 1024);
      } catch (IOException e) {
        writerError.set(e);
      } finally {
        writerDone.countDown();
      }
    });

    writer.start();
    assertWaitUntil(() -> writer.getState() == Thread.State.WAITING);

    sink.fail(new RuntimeException("sink error"));

    assertTrue("Writer must unblock within 5s", writerDone.await(5, TimeUnit.SECONDS));
    assertNotNull("Writer should receive an IOException", writerError.get());
  }
}
