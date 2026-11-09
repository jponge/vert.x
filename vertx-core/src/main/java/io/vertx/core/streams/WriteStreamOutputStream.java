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
package io.vertx.core.streams;

import io.vertx.core.Context;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.impl.Utils;
import io.vertx.core.impl.WorkerExecutor;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Adapts a Vert.x {@link WriteStream} of {@link Buffer} to a blocking {@link OutputStream}.
 * <p>
 * This bridge allows blocking producers to write to async Vert.x streams. Can be called from
 * any thread type, including event-loop threads (where back-pressure is not enforced).
 * Thread-safety: single producer thread assumed. Concurrent writes from multiple threads
 * are not supported.
 * <p>
 * Small writes are accumulated in an internal buffer and sent to the sink on {@link #flush()}
 * or {@link #close()}. Large writes (>= directWriteThreshold) bypass the accumulator and go
 * directly to the sink.
 * <p>
 * On worker/virtual threads, back-pressure blocks the caller when the sink's write queue is full.
 * On event-loop threads, writes return immediately without back-pressure enforcement.
 */
public class WriteStreamOutputStream extends OutputStream {

  private final WriteStream<Buffer> sink;
  private Buffer accumulator;
  private final int directWriteThreshold;
  private Throwable error;
  private boolean closed;
  private final ReentrantLock lock;
  private final Condition drainCondition;

  /**
   * Create a new WriteStreamOutputStream.
   *
   * @param sink the sink WriteStream
   * @param directWriteThreshold writes >= this size bypass the accumulator
   */
  public WriteStreamOutputStream(WriteStream<Buffer> sink, int directWriteThreshold) {
    this.sink = sink;
    this.directWriteThreshold = directWriteThreshold;
    this.accumulator = Buffer.buffer(directWriteThreshold);
    this.lock = new ReentrantLock();
    this.drainCondition = lock.newCondition();

    sink.drainHandler(v -> {
      lock.lock();
      try {
        drainCondition.signalAll();
      } finally {
        lock.unlock();
      }
    });

    sink.exceptionHandler(err -> {
      lock.lock();
      try {
        if (error == null) {
          error = err;
        }
        drainCondition.signalAll();
      } finally {
        lock.unlock();
      }
    });
  }

  @Override
  public void write(int b) throws IOException {
    lock.lock();
    try {
      checkClosed();
      checkError();
      accumulator.appendByte((byte) b);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException();
    }
    if (len == 0) {
      return;
    }

    if (len >= directWriteThreshold) {
      // Flush accumulator first to preserve ordering, then write large data directly
      Buffer toFlush = null;
      lock.lock();
      try {
        checkClosed();
        checkError();
        if (accumulator.length() > 0) {
          toFlush = accumulator;
          accumulator = Buffer.buffer(directWriteThreshold);
        }
      } finally {
        lock.unlock();
      }
      if (toFlush != null) {
        writeToSink(toFlush);
      }
      writeToSink(Buffer.buffer(len).appendBytes(b, off, len));
    } else {
      // Accumulate small writes
      lock.lock();
      try {
        checkClosed();
        checkError();
        accumulator.appendBytes(b, off, len);
      } finally {
        lock.unlock();
      }
    }
  }

  @Override
  public void flush() throws IOException {
    Buffer toFlush;
    lock.lock();
    try {
      checkClosed();
      checkError();
      if (accumulator.length() == 0) {
        return;
      }
      toFlush = accumulator;
      accumulator = Buffer.buffer(directWriteThreshold);
    } finally {
      lock.unlock();
    }
    writeToSink(toFlush);
  }

  @Override
  public void close() throws IOException {
    Buffer toFlush = null;
    lock.lock();
    try {
      if (closed) {
        return;
      }
      closed = true;
      if (accumulator.length() > 0) {
        toFlush = accumulator;
      }
      accumulator = null;
      drainCondition.signalAll();
    } finally {
      lock.unlock();
    }
    if (toFlush != null) {
      writeToSink(toFlush);
    }
    sink.end();
  }

  private void checkClosed() throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }
  }

  private void checkError() throws IOException {
    if (error != null) {
      throw new IOException(error);
    }
  }

  private void writeToSink(Buffer buffer) throws IOException {
    // Capture write errors
    sink.write(buffer).onFailure(err -> {
      lock.lock();
      try {
        error = err;
        drainCondition.signalAll();
      } finally {
        lock.unlock();
      }
    });

    // Back-pressure: block on worker/virtual threads, return immediately on event-loop
    if (!Context.isOnEventLoopThread()) {
      lock.lock();
      try {
        while (sink.writeQueueFull() && error == null && !closed) {
          awaitDrain();
        }
        if (error != null) {
          throw new IOException(error);
        }
      } finally {
        lock.unlock();
      }
    }
  }

  private void awaitDrain() {
    WorkerExecutor executor = WorkerExecutor.unwrapWorkerExecutor();
    if (executor != null) {
      WorkerExecutor.Execution execution = executor.currentExecution();
      CountDownLatch latch = execution.trySuspend();
      try {
        drainCondition.await();
        lock.unlock();
        execution.resume();
        latch.await();
        lock.lock();
      } catch (InterruptedException e) {
        Utils.throwAsUnchecked(e);
      }
    } else {
      try {
        drainCondition.await();
      } catch (InterruptedException e) {
        Utils.throwAsUnchecked(e);
      }
    }
  }
}
