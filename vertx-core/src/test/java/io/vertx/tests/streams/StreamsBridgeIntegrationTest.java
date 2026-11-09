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

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.streams.JavaIOStreams;
import io.vertx.test.core.VertxTestBase;
import io.vertx.test.fakestream.FakeStream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public class StreamsBridgeIntegrationTest extends VertxTestBase {

  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();

  // Test: factory creates working InputStream
  @Test
  public void testInputStreamFactory() throws Exception {
    FakeStream<Buffer> source = new FakeStream<>();
    try (InputStream is = JavaIOStreams.inputStream(source)) {
      assertNotNull(is);
      source.write(Buffer.buffer("test"));
      source.end();
      byte[] buf = new byte[4];
      assertEquals(4, is.read(buf, 0, 4));
      assertEquals("test", new String(buf));
    }
  }

  // Test: factory creates working OutputStream
  @Test
  public void testOutputStreamFactory() throws Exception {
    FakeStream<Buffer> sink = new FakeStream<>();
    List<Buffer> written = new ArrayList<>();
    sink.handler(written::add);
    try (OutputStream os = JavaIOStreams.outputStream(sink)) {
      assertNotNull(os);
      os.write("test".getBytes());
      os.flush();
      assertEquals(1, written.size());
    }
  }

  // Test: custom highWaterMark is respected
  @Test
  public void testInputStreamCustomHighWaterMark() throws IOException {
    FakeStream<Buffer> source = new FakeStream<>();
    try (InputStream ignored = JavaIOStreams.inputStream(source, 50)) {
      source.write(Buffer.buffer(new byte[60]));
      assertTrue(source.isPaused());
    }
  }

  // Test: custom directWriteThreshold is respected
  @Test
  public void testOutputStreamCustomThreshold() throws Exception {
    FakeStream<Buffer> sink = new FakeStream<>();
    List<Buffer> written = new ArrayList<>();
    sink.handler(written::add);
    try (OutputStream os = JavaIOStreams.outputStream(sink, 10)) {
      os.write(new byte[20]); // >= threshold, should go direct
      assertEquals(1, written.size());
    }
  }

  // Test: read an AsyncFile through InputStream
  @Test
  public void testReadAsyncFileViaInputStream() throws Exception {
    String content = "The quick brown fox jumps over the lazy dog";
    Vertx vertx = this.vertx;
    String path = testFolder.newFile("test.txt").getAbsolutePath();
    vertx.fileSystem().writeFileBlocking(path, Buffer.buffer(content));

    AsyncFile file = vertx.fileSystem().openBlocking(path, new OpenOptions());

    String[] result = new String[1];
    Thread reader = new Thread(() -> {
      try (InputStream is = JavaIOStreams.inputStream(file)) {
        byte[] buf = new byte[1024];
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        int n;
        while ((n = is.read(buf)) != -1) {
          baos.write(buf, 0, n);
        }
        result[0] = baos.toString();
        testComplete();
      } catch (IOException e) {
        fail(e.getMessage());
      }
    });
    reader.start();
    await();
    assertEquals(content, result[0]);
  }

  // Test: write to an AsyncFile through OutputStream
  @Test
  public void testWriteAsyncFileViaOutputStream() throws Exception {
    String path = testFolder.newFile("out.txt").getAbsolutePath();
    AsyncFile file = vertx.fileSystem().openBlocking(path, new OpenOptions());

    String content = "Hello from OutputStream bridge";
    Thread writer = new Thread(() -> {
      try (OutputStream os = JavaIOStreams.outputStream(file)) {
        os.write(content.getBytes());
        testComplete();
      } catch (IOException e) {
        fail(e.getMessage());
      }
    });
    writer.start();
    await();
    // Small delay to ensure async file operations complete
    Thread.sleep(100);
    Buffer result = vertx.fileSystem().readFileBlocking(path);
    assertEquals(content, result.toString());
  }

  // Test: HTTP request body read via InputStream, response written via OutputStream
  @Test
  public void testHttpRoundTrip() {
    byte[] data = new byte[256 * 1024];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) (i % 251);
    }
    Buffer body = Buffer.buffer(data);

    HttpServer server = vertx.createHttpServer();
    server.requestHandler(req -> {
      req.pause();
      new Thread(() -> {
        try {
          ByteArrayOutputStream baos = new ByteArrayOutputStream();
          try (InputStream is = JavaIOStreams.inputStream(req)) {
            byte[] buf = new byte[4096];
            int n;
            while ((n = is.read(buf)) != -1) {
              baos.write(buf, 0, n);
            }
          }
          try (OutputStream os = JavaIOStreams.outputStream(req.response())) {
            os.write(baos.toByteArray());
          }
        } catch (IOException e) {
          req.response().setStatusCode(500).end();
        }
      }).start();
    });
    server.listen(0, "localhost").await();
    int port = server.actualPort();

    HttpClient client = vertx.createHttpClient();
    Buffer response = client
      .request(HttpMethod.POST, port, "localhost", "/")
      .compose(req -> req.send(body))
      .compose(HttpClientResponse::body)
      .await();
    assertEquals(body, response);
    server.close().await();
  }
}
