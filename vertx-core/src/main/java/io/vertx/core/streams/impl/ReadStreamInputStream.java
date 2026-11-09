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
package io.vertx.core.streams.impl;

import io.vertx.core.Context;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.impl.WorkerExecutor;
import io.vertx.core.streams.ReadStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.util.ArrayDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Adapts a Vert.x {@link ReadStream} of {@link Buffer} to a blocking {@link InputStream}.
 * <p>
 * This bridge allows blocking consumers to read from async Vert.x streams. Must not be used
 * from event-loop threads.
 * <p>
 * Back-pressure is managed via high/low water marks: the source is paused when pending bytes
 * reach the high water mark and resumed when they drop below the low water mark.
 */
public class ReadStreamInputStream extends InputStream {

  private static final int ACTIVE = 0;
  private static final int ENDED = 1;
  private static final int ERRORED = 2;
  private static final int CLOSED = 3;

  private final ReadStream<Buffer> source;
  private final ArrayDeque<Buffer> queue;

  private final long highWaterMark;
  private final long lowWaterMark;

  private int state;
  private boolean paused;
  private Throwable error;

  private Buffer current;
  private int currentOffset;
  private int currentBytesDeducted;
  private long pendingBytes;

  private final long timeout;
  private final TimeUnit timeoutUnit;
  private final Runnable onCloseBeforeEnd;

  private final ReentrantLock lock;
  private final Condition dataAvailable;

  /**
   * Create a new ReadStreamInputStream.
   *
   * @param source the source ReadStream
   * @param highWaterMark maximum bytes buffered before pausing the source
   */
  public ReadStreamInputStream(ReadStream<Buffer> source, int highWaterMark) {
    this(source, highWaterMark, null, 0, TimeUnit.MILLISECONDS, null);
  }

  /**
   * Create a new ReadStreamInputStream with a read timeout.
   *
   * @param source the source ReadStream
   * @param highWaterMark maximum bytes buffered before pausing the source
   * @param timeout the read timeout value, 0 means no timeout
   * @param unit the time unit for the timeout
   */
  public ReadStreamInputStream(ReadStream<Buffer> source, int highWaterMark,
                                long timeout, TimeUnit unit) {
    this(source, highWaterMark, null, timeout, unit, null);
  }

  /**
   * Create a new ReadStreamInputStream with initial data prepended.
   *
   * @param source the source ReadStream
   * @param highWaterMark maximum bytes buffered before pausing the source
   * @param initialData data to prepend before any data from the source, may be {@code null}
   */
  public ReadStreamInputStream(ReadStream<Buffer> source, int highWaterMark,
                                Buffer initialData) {
    this(source, highWaterMark, initialData, 0, TimeUnit.MILLISECONDS, null);
  }

  /**
   * Create a new ReadStreamInputStream with all options.
   *
   * @param source the source ReadStream
   * @param highWaterMark maximum bytes buffered before pausing the source
   * @param initialData data to prepend before any data from the source, may be {@code null}
   * @param timeout the read timeout value, 0 means no timeout
   * @param unit the time unit for the timeout
   * @param onCloseBeforeEnd callback invoked when the stream is closed before the source ends, may be {@code null}
   */
  public ReadStreamInputStream(ReadStream<Buffer> source, int highWaterMark,
                                Buffer initialData,
                                long timeout, TimeUnit unit,
                                Runnable onCloseBeforeEnd) {
    this.source = source;
    this.highWaterMark = highWaterMark;
    this.lowWaterMark = highWaterMark / 4;
    this.timeout = timeout;
    this.timeoutUnit = unit;
    this.onCloseBeforeEnd = onCloseBeforeEnd;
    this.queue = new ArrayDeque<>();
    this.lock = new ReentrantLock();
    this.dataAvailable = lock.newCondition();

    source.pause();
    source.handler(this::handleData);
    source.endHandler(v -> handleEnd());
    source.exceptionHandler(this::handleError);
    source.resume();

    prependInitialData(initialData);
  }

  private void prependInitialData(Buffer initialData) {
    if (initialData != null && initialData.length() > 0) {
      lock.lock();
      try {
        if (current != null) {
          queue.addFirst(current);
        }
        current = initialData;
        currentOffset = 0;
        currentBytesDeducted = 0;
        pendingBytes += initialData.length();
        if (!paused && pendingBytes >= highWaterMark) {
          source.pause();
          paused = true;
        }
      } finally {
        lock.unlock();
      }
    }
  }

  private void handleData(Buffer buffer) {
    if (buffer.length() == 0) {
      return;
    }
    lock.lock();
    try {
      if (state == CLOSED) {
        return;
      }
      queue.add(buffer);
      pendingBytes += buffer.length();
      if (!paused && pendingBytes >= highWaterMark) {
        source.pause();
        paused = true;
      }
      dataAvailable.signal();
    } finally {
      lock.unlock();
    }
  }

  private void handleEnd() {
    lock.lock();
    try {
      if (state < ENDED) {
        state = ENDED;
      }
      dataAvailable.signalAll();
    } finally {
      lock.unlock();
    }
  }

  private void handleError(Throwable err) {
    lock.lock();
    try {
      if (state < ERRORED) {
        state = ERRORED;
        error = err;
      }
      dataAvailable.signalAll();
    } finally {
      lock.unlock();
    }
  }

  private void checkEventLoop() {
    if (Context.isOnEventLoopThread()) {
      throw new IllegalStateException("ReadStreamInputStream cannot be used from event-loop threads");
    }
  }

  /**
   * Ensures {@code current} is non-null by dequeuing from the queue, blocking if necessary.
   *
   * @return the current buffer, or {@code null} if the stream has ended and the queue is empty
   */
  private Buffer ensureCurrent() throws IOException {
    Buffer cur = current;
    if (cur != null) {
      return cur;
    }
    lock.lock();
    try {
      while (queue.isEmpty() && state == ACTIVE) {
        awaitProgress();
      }
      if (state == CLOSED) {
        throw new IOException("Stream closed");
      }
      if (state == ERRORED) {
        throw new IOException(error);
      }
      if (state == ENDED && queue.isEmpty()) {
        return null;
      }
      current = queue.poll();
      currentOffset = 0;
      currentBytesDeducted = 0;
      return current;
    } finally {
      lock.unlock();
    }
  }

  private void releaseConsumedBuffer(int bufferLength) {
    int remaining = bufferLength - currentBytesDeducted;
    current = null;
    currentOffset = 0;
    currentBytesDeducted = 0;
    if (remaining > 0) {
      lock.lock();
      try {
        pendingBytes -= remaining;
        if (paused && pendingBytes <= lowWaterMark) {
          source.resume();
          paused = false;
        }
      } finally {
        lock.unlock();
      }
    }
  }

  @Override
  public int read() throws IOException {
    checkEventLoop();

    Buffer cur = ensureCurrent();
    if (cur == null) {
      return -1;
    }

    byte b = cur.getByte(currentOffset++);

    if (currentOffset >= cur.length()) {
      releaseConsumedBuffer(cur.length());
    }

    return b & 0xFF;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    checkEventLoop();

    if (b == null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException();
    }
    if (len == 0) {
      return 0;
    }

    Buffer cur = ensureCurrent();
    if (cur == null) {
      return -1;
    }

    int available = cur.length() - currentOffset;
    int n = Math.min(available, len);
    cur.getBytes(currentOffset, currentOffset + n, b, off);
    currentOffset += n;

    if (currentOffset >= cur.length()) {
      releaseConsumedBuffer(cur.length());
    } else {
      currentBytesDeducted += n;
      lock.lock();
      try {
        pendingBytes -= n;
        if (paused && pendingBytes <= lowWaterMark) {
          source.resume();
          paused = false;
        }
      } finally {
        lock.unlock();
      }
    }

    return n;
  }

  @Override
  public int available() throws IOException {
    lock.lock();
    try {
      if (state == CLOSED) {
        throw new IOException("Stream closed");
      }
      return (int) Math.min(pendingBytes, Integer.MAX_VALUE);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void close() throws IOException {
    boolean wasActive;
    lock.lock();
    try {
      if (state == CLOSED) {
        return;
      }
      wasActive = (state == ACTIVE);
      state = CLOSED;
      try {
        if (paused) {
          source.resume();
          paused = false;
        }
        source.handler(null);
        source.endHandler(null);
        source.exceptionHandler(null);
      } catch (Exception ignored) {
      }
      queue.clear();
      current = null;
      pendingBytes = 0;
      dataAvailable.signalAll();
    } finally {
      lock.unlock();
    }
    if (wasActive && onCloseBeforeEnd != null) {
      try {
        onCloseBeforeEnd.run();
      } catch (Exception ignored) {
      }
    }
  }

  private void awaitProgress() throws IOException {
    WorkerExecutor executor = WorkerExecutor.unwrapWorkerExecutor();
    if (executor != null) {
      WorkerExecutor.Execution execution = executor.currentExecution();
      CountDownLatch latch = execution.trySuspend();
      try {
        boolean signaled = awaitWithTimeout(dataAvailable);
        lock.unlock();
        execution.resume();
        latch.await();
        lock.lock();
        if (!signaled) {
          throw new InterruptedIOException("Read timed out");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new InterruptedIOException();
      }
    } else {
      try {
        if (!awaitWithTimeout(dataAvailable)) {
          throw new InterruptedIOException("Read timed out");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new InterruptedIOException();
      }
    }
  }

  private boolean awaitWithTimeout(Condition condition) throws InterruptedException {
    if (timeout > 0) {
      return condition.await(timeout, timeoutUnit);
    }
    condition.await();
    return true;
  }
}
