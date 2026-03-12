/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.gcp.gcs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.api.client.http.apache.v2.ApacheHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.cloud.gcs.analyticscore.client.GcsClientOptions;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystem;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystemOptions;
import com.google.cloud.gcs.analyticscore.client.GcsReadOptions;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.StorageOptions;
import java.util.Map;
import org.apache.iceberg.EnvironmentContext;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

@SuppressWarnings("resource")
public class TestPrefixedStorage {

  @Test
  public void invalidParameters() {
    assertThatThrownBy(() -> new PrefixedStorage(null, null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid storage prefix: null or empty");

    assertThatThrownBy(() -> new PrefixedStorage("", null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid storage prefix: null or empty");

    assertThatThrownBy(() -> new PrefixedStorage("gs://bucket", null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid properties: null");
  }

  @Test
  public void validParameters() {
    Map<String, String> properties =
        ImmutableMap.of(
            GCPProperties.GCS_PROJECT_ID, "myProject", GCPProperties.GCS_OAUTH2_TOKEN, "token");
    PrefixedStorage storage = new PrefixedStorage("gs://bucket", properties, null);

    assertThat(storage.storage()).isNotNull();
    assertThat(storage.storagePrefix()).isEqualTo("gs://bucket");
    assertThat(storage.gcpProperties().properties()).isEqualTo(properties);
  }

  @Test
  public void userAgentPrefix() {
    Map<String, String> properties =
        ImmutableMap.of(
            GCPProperties.GCS_PROJECT_ID, "myProject",
            GCPProperties.GCS_OAUTH2_TOKEN, "token",
            GCPProperties.GCS_USER_PROJECT, "myUserProject");
    PrefixedStorage storage = new PrefixedStorage("gs://bucket", properties, null);

    assertThat(storage.storage().getOptions().getUserAgent())
        .isEqualTo("gcsfileio/" + EnvironmentContext.get());
  }

  @Test
  public void impersonationPropertiesAreRead() {
    Map<String, String> properties =
        ImmutableMap.of(
            GCPProperties.GCS_PROJECT_ID,
            "myProject",
            GCPProperties.GCS_IMPERSONATE_SERVICE_ACCOUNT,
            "test-sa@project.iam.gserviceaccount.com",
            GCPProperties.GCS_IMPERSONATE_DELEGATES,
            "delegate-sa@project.iam.gserviceaccount.com",
            GCPProperties.GCS_IMPERSONATE_LIFETIME_SECONDS,
            "1800",
            GCPProperties.GCS_IMPERSONATE_SCOPES,
            "bigquery,devstorage.read_only");

    GCPProperties gcpProperties = new GCPProperties(properties);

    assertThat(gcpProperties.impersonateServiceAccount())
        .contains("test-sa@project.iam.gserviceaccount.com");
    assertThat(gcpProperties.impersonateDelegates())
        .contains("delegate-sa@project.iam.gserviceaccount.com");
    assertThat(gcpProperties.impersonateLifetimeSeconds()).isEqualTo(1800);
    assertThat(gcpProperties.impersonateScopes())
        .containsExactly(
            "https://www.googleapis.com/auth/bigquery",
            "https://www.googleapis.com/auth/devstorage.read_only");
  }

  @Test
  public void impersonationPropertiesWithDefaults() {
    Map<String, String> properties =
        ImmutableMap.of(
            GCPProperties.GCS_PROJECT_ID,
            "myProject",
            GCPProperties.GCS_IMPERSONATE_SERVICE_ACCOUNT,
            "test-sa@project.iam.gserviceaccount.com");

    GCPProperties gcpProperties = new GCPProperties(properties);

    assertThat(gcpProperties.impersonateServiceAccount())
        .contains("test-sa@project.iam.gserviceaccount.com");
    assertThat(gcpProperties.impersonateDelegates()).isNull();
    assertThat(gcpProperties.impersonateLifetimeSeconds())
        .isEqualTo(GCPProperties.GCS_IMPERSONATE_LIFETIME_SECONDS_DEFAULT);
  }

  @Test
  public void gcsFileSystem() {
    Map<String, String> properties =
        ImmutableMap.of(
            GCPProperties.GCS_PROJECT_ID, "myProject",
            GCPProperties.GCS_USER_PROJECT, "userProject",
            GCPProperties.GCS_CLIENT_LIB_TOKEN, "gccl",
            GCPProperties.GCS_SERVICE_HOST, "example.com",
            GCPProperties.GCS_DECRYPTION_KEY, "decryptionKey",
            GCPProperties.GCS_ENCRYPTION_KEY, "encryptionKey",
            GCPProperties.GCS_CHANNEL_READ_CHUNK_SIZE, "1024");
    PrefixedStorage storage = new PrefixedStorage("gs://bucket", properties, null);
    GcsFileSystemOptions expectedOptions =
        GcsFileSystemOptions.builder()
            .setGcsClientOptions(
                GcsClientOptions.builder()
                    .setProjectId("myProject")
                    .setClientLibToken("gccl")
                    .setServiceHost("example.com")
                    .setUserAgent("gcsfileio/" + EnvironmentContext.get())
                    .setGcsReadOptions(
                        GcsReadOptions.builder()
                            .setChunkSize(1024)
                            .setDecryptionKey("decryptionKey")
                            .setUserProjectId("userProject")
                            .build())
                    .build())
            .build();

    GcsFileSystem fileSystem = storage.gcsFileSystem();

    assertThat(fileSystem).isNotNull();
    assertThat(fileSystem.getGcsClient()).isNotNull();
    assertThat(fileSystem.getFileSystemOptions()).isEqualTo(expectedOptions);
  }

  @Test
  public void testDefaultHttpTransportClass() {
    Map<String, String> properties = ImmutableMap.of(GCPProperties.GCS_PROJECT_ID, "myProject");
    PrefixedStorage storage = new PrefixedStorage("gs://bucket", properties, null);
    StorageOptions options = storage.storage().getOptions();
    HttpTransportOptions httpOptions = (HttpTransportOptions) options.getTransportOptions();
    assertThat(httpOptions.getHttpTransportFactory().create())
        .describedAs("Default HTTP transport should be NetHttpTransport")
        .isInstanceOf(NetHttpTransport.class);
  }

  @Test
  public void testHttpTransportOptions() {
    Map<String, String> properties =
        ImmutableMap.of(
            GCPProperties.GCS_PROJECT_ID, "myProject",
            GCPProperties.GCS_CONNECTION_TIMEOUT_MILLIS, "1000",
            GCPProperties.GCS_READ_TIMEOUT_MILLIS, "2000");
    PrefixedStorage storage = new PrefixedStorage("gs://bucket", properties, null);
    StorageOptions options = storage.storage().getOptions();
    assertThat(options.getTransportOptions()).isInstanceOf(HttpTransportOptions.class);
    HttpTransportOptions httpOptions = (HttpTransportOptions) options.getTransportOptions();
    assertThat(httpOptions.getHttpTransportFactory().create())
        .describedAs(
            "HTTP transport should be NetHttpTransport when connection or read timeout is set")
        .isInstanceOf(NetHttpTransport.class);
    assertThat(httpOptions.getConnectTimeout())
        .describedAs("Connection timeout should be set from properties")
        .isEqualTo(1000);
    assertThat(httpOptions.getReadTimeout())
        .describedAs("Read timeout should be set from properties")
        .isEqualTo(2000);
  }

  @Test
  public void testMaxConnections() {
    Map<String, String> properties =
        ImmutableMap.of(
            GCPProperties.GCS_PROJECT_ID, "myProject",
            GCPProperties.GCS_CONNECTION_TIMEOUT_MILLIS, "10000",
            GCPProperties.GCS_READ_TIMEOUT_MILLIS, "30000",
            GCPProperties.GCS_MAX_CONNECTIONS, "10");
    PrefixedStorage storage = new PrefixedStorage("gs://bucket", properties, null);
    StorageOptions options = storage.storage().getOptions();
    assertThat(options.getTransportOptions()).isInstanceOf(HttpTransportOptions.class);
    HttpTransportOptions httpOptions = (HttpTransportOptions) options.getTransportOptions();
    assertThat(httpOptions.getHttpTransportFactory().create())
        .describedAs("HTTP transport should be ApacheHttpTransport when max connections is set")
        .isInstanceOf(ApacheHttpTransport.class);
    // Figuring out if we can match the value max connection.
  }
}
