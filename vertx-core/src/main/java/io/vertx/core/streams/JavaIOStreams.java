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

import io.vertx.core.buffer.Buffer;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Utility methods for bridging between Vert.x async streams and blocking I/O streams.
 */
public interface JavaIOStreams {

  int DEFAULT_HIGH_WATER_MARK = 65536;
  int DEFAULT_DIRECT_WRITE_THRESHOLD = 8192;

  /**
   * Create an {@link InputStream} that reads from a Vert.x {@link ReadStream} of {@link Buffer}.
   * Uses a default high water mark of 64 KB.
   *
   * @param source the source ReadStream
   * @return an InputStream that reads from the source
   */
  static InputStream inputStream(ReadStream<Buffer> source) {
    return new ReadStreamInputStream(source, DEFAULT_HIGH_WATER_MARK);
  }

  /**
   * Create an {@link InputStream} that reads from a Vert.x {@link ReadStream} of {@link Buffer}.
   *
   * @param source the source ReadStream
   * @param highWaterMark maximum bytes buffered before pausing the source
   * @return an InputStream that reads from the source
   */
  static InputStream inputStream(ReadStream<Buffer> source, int highWaterMark) {
    return new ReadStreamInputStream(source, highWaterMark);
  }

  /**
   * Create an {@link OutputStream} that writes to a Vert.x {@link WriteStream} of {@link Buffer}.
   * Uses a default direct write threshold of 8 KB.
   *
   * @param sink the sink WriteStream
   * @return an OutputStream that writes to the sink
   */
  static OutputStream outputStream(WriteStream<Buffer> sink) {
    return new WriteStreamOutputStream(sink, DEFAULT_DIRECT_WRITE_THRESHOLD);
  }

  /**
   * Create an {@link OutputStream} that writes to a Vert.x {@link WriteStream} of {@link Buffer}.
   *
   * @param sink the sink WriteStream
   * @param directWriteThreshold writes >= this size bypass the accumulator
   * @return an OutputStream that writes to the sink
   */
  static OutputStream outputStream(WriteStream<Buffer> sink, int directWriteThreshold) {
    return new WriteStreamOutputStream(sink, directWriteThreshold);
  }
}
