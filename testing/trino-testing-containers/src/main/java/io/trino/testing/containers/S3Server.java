/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.testing.containers;

import com.google.common.net.HostAndPort;
import com.google.common.reflect.ClassPath;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.trino.testing.minio.MinioClient;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.regex.Pattern;

import static java.time.temporal.ChronoUnit.MINUTES;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.regex.Matcher.quoteReplacement;

public interface S3Server
        extends AutoCloseable
{
    String ACCESS_KEY = "accesskey";
    String SECRET_KEY = "secretkey";
    String REGION = "us-east-1";

    void start();

    HostAndPort getApiEndpoint();

    default String getAddress()
    {
        return "http://" + getApiEndpoint();
    }

    default void createBucket(String bucketName)
    {
        try (MinioClient minioClient = createMinioClient()) {
            // use retry loop for minioClient.makeBucket as minio container tends to return "Server not initialized, please try again" error
            // for some time after starting up
            RetryPolicy<Object> retryPolicy = RetryPolicy.builder()
                    .withMaxDuration(Duration.of(2, MINUTES))
                    .withMaxAttempts(Integer.MAX_VALUE) // limited by MaxDuration
                    .withDelay(Duration.of(10, SECONDS))
                    .build();
            Failsafe.with(retryPolicy).run(() -> minioClient.makeBucket(bucketName));
        }
    }

    default void copyResources(String resourcePath, String bucketName, String target)
    {
        try (MinioClient minioClient = createMinioClient()) {
            for (ClassPath.ResourceInfo resourceInfo : ClassPath.from(MinioClient.class.getClassLoader())
                    .getResources()) {
                if (resourceInfo.getResourceName().startsWith(resourcePath)) {
                    String fileName = resourceInfo.getResourceName().replaceFirst("^" + Pattern.quote(resourcePath), quoteReplacement(target));
                    minioClient.putObject(bucketName, resourceInfo.asByteSource().read(), fileName);
                }
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    default void writeFile(byte[] contents, String bucketName, String path)
    {
        try (MinioClient minioClient = createMinioClient()) {
            minioClient.putObject(bucketName, contents, path);
        }
    }

    default MinioClient createMinioClient()
    {
        return new MinioClient(getAddress(), ACCESS_KEY, SECRET_KEY);
    }
}
