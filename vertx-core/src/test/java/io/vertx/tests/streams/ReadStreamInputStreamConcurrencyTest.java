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
import io.vertx.core.streams.ReadStreamInputStream;
import io.vertx.test.core.VertxTestBase;
import io.vertx.test.fakestream.FakeStream;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;

public class ReadStreamInputStreamConcurrencyTest extends VertxTestBase {

  @Test
  public void testDataIntegrityUnderLoad() throws Exception {
    FakeStream<Buffer> source = new FakeStream<>();
    ReadStreamInputStream is = new ReadStreamInputStream(source, 1024);

    AtomicReference<Throwable> producerError = new AtomicReference<>();
    AtomicReference<Throwable> consumerError = new AtomicReference<>();
    AtomicReference<int[]> receivedData = new AtomicReference<>();

    Thread producer = new Thread(() -> {
      try {
        for (int i = 0; i < 10000; i++) {
          Buffer buf = Buffer.buffer(4);
          buf.setInt(0, i);
          source.write(buf);
        }
        source.end();
      } catch (Throwable t) {
        producerError.set(t);
      }
    });

    Thread consumer = new Thread(() -> {
      try {
        int[] data = new int[10000];
        byte[] buf = new byte[4];
        int index = 0;
        while (true) {
          int n = is.read(buf, 0, 4);
          if (n == -1) {
            break;
          }
          if (n == 4) {
            int value = ((buf[0] & 0xFF) << 24) |
                        ((buf[1] & 0xFF) << 16) |
                        ((buf[2] & 0xFF) << 8) |
                        (buf[3] & 0xFF);
            data[index++] = value;
          } else {
            int offset = n;
            while (offset < 4) {
              int extra = is.read(buf, offset, 4 - offset);
              if (extra == -1) {
                throw new IOException("Unexpected end of stream");
              }
              offset += extra;
            }
            int value = ((buf[0] & 0xFF) << 24) |
                        ((buf[1] & 0xFF) << 16) |
                        ((buf[2] & 0xFF) << 8) |
                        (buf[3] & 0xFF);
            data[index++] = value;
          }
        }
        receivedData.set(data);
      } catch (Throwable t) {
        consumerError.set(t);
      }
    });

    producer.start();
    consumer.start();

    producer.join(30000);
    consumer.join(30000);

    assertFalse("Producer thread still alive", producer.isAlive());
    assertFalse("Consumer thread still alive", consumer.isAlive());
    assertNull("Producer error: " + producerError.get(), producerError.get());
    assertNull("Consumer error: " + consumerError.get(), consumerError.get());

    int[] data = receivedData.get();
    assertNotNull("No data received", data);
    for (int i = 0; i < 10000; i++) {
      assertEquals("Data mismatch at index " + i, i, data[i]);
    }
  }

  @Test
  public void testCloseWhileReadingIterated() throws Exception {
    Random random = new Random(42);

    for (int iteration = 0; iteration < 500; iteration++) {
      FakeStream<Buffer> source = new FakeStream<>();
      ReadStreamInputStream is = new ReadStreamInputStream(source, 256);

      for (int i = 0; i < 10; i++) {
        source.write(Buffer.buffer(new byte[50]));
      }

      AtomicReference<Throwable> unexpectedError = new AtomicReference<>();

      Thread reader = new Thread(() -> {
        try {
          byte[] buf = new byte[128];
          while (true) {
            int n = is.read(buf, 0, buf.length);
            if (n == -1) {
              break;
            }
          }
        } catch (IOException expected) {
          // Expected when stream is closed
        } catch (Throwable t) {
          unexpectedError.set(t);
        }
      });

      reader.start();

      int sleepTime = random.nextInt(3);
      if (sleepTime > 0) {
        Thread.sleep(sleepTime);
      }

      is.close();
      reader.join(5000);

      assertFalse("Reader thread still alive at iteration " + iteration, reader.isAlive());
      assertNull("Unexpected error at iteration " + iteration + ": " + unexpectedError.get(), unexpectedError.get());
    }
  }

  @Test
  public void testBackPressureCyclingIntegrity() throws Exception {
    FakeStream<Buffer> source = new FakeStream<>();
    ReadStreamInputStream is = new ReadStreamInputStream(source, 100);

    AtomicReference<Throwable> producerError = new AtomicReference<>();
    AtomicReference<Throwable> consumerError = new AtomicReference<>();
    AtomicReference<Integer> totalBytesRead = new AtomicReference<>(0);

    Thread producer = new Thread(() -> {
      try {
        for (int i = 0; i < 500; i++) {
          source.write(Buffer.buffer(new byte[30]));
        }
        source.end();
      } catch (Throwable t) {
        producerError.set(t);
      }
    });

    Thread consumer = new Thread(() -> {
      try {
        byte[] buf = new byte[128];
        int total = 0;
        while (true) {
          int n = is.read(buf, 0, buf.length);
          if (n == -1) {
            break;
          }
          total += n;
        }
        totalBytesRead.set(total);
      } catch (Throwable t) {
        consumerError.set(t);
      }
    });

    producer.start();
    consumer.start();

    producer.join(30000);
    consumer.join(30000);

    assertFalse("Producer thread still alive", producer.isAlive());
    assertFalse("Consumer thread still alive", consumer.isAlive());
    assertNull("Producer error: " + producerError.get(), producerError.get());
    assertNull("Consumer error: " + consumerError.get(), consumerError.get());

    int expected = 500 * 30;
    assertEquals("Total bytes read", expected, totalBytesRead.get().intValue());
    assertTrue("Source should have been paused at least once", source.pauseCount() > 0);
  }

  @Test
  public void testConcurrentProducerConsumerThroughput() throws Exception {
    FakeStream<Buffer> source = new FakeStream<>();
    ReadStreamInputStream is = new ReadStreamInputStream(source, 4096);

    CyclicBarrier barrier = new CyclicBarrier(2);
    AtomicReference<Throwable> producerError = new AtomicReference<>();
    AtomicReference<Throwable> consumerError = new AtomicReference<>();
    AtomicReference<Long> totalBytesRead = new AtomicReference<>(0L);

    Thread producer = new Thread(() -> {
      try {
        barrier.await();
        for (int i = 0; i < 100000; i++) {
          source.write(Buffer.buffer(new byte[64]));
        }
        source.end();
      } catch (Throwable t) {
        producerError.set(t);
      }
    });

    Thread consumer = new Thread(() -> {
      try {
        barrier.await();
        byte[] buf = new byte[1024];
        long total = 0;
        while (true) {
          int n = is.read(buf, 0, buf.length);
          if (n == -1) {
            break;
          }
          total += n;
        }
        totalBytesRead.set(total);
      } catch (Throwable t) {
        consumerError.set(t);
      }
    });

    producer.start();
    consumer.start();

    producer.join(30000);
    consumer.join(30000);

    assertFalse("Producer thread still alive", producer.isAlive());
    assertFalse("Consumer thread still alive", consumer.isAlive());
    assertNull("Producer error: " + producerError.get(), producerError.get());
    assertNull("Consumer error: " + consumerError.get(), consumerError.get());

    long expected = 100000L * 64L;
    assertEquals("Total bytes read", expected, totalBytesRead.get().longValue());
  }
}
