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
package io.trino.plugin.hive.containers;

import io.trino.testing.ResourcePresence;
import io.trino.testing.containers.S3Server;
import io.trino.testing.minio.MinioClient;
import io.trino.util.AutoCloseableCloser;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public abstract class HiveS3DataLake<T extends HiveS3DataLake, S extends S3Server>
        implements AutoCloseable
{
    protected final String bucketName;
    protected final HiveHadoop hiveHadoop;

    private final S s3server;

    private final AutoCloseableCloser closer = AutoCloseableCloser.create();

    private State state = State.INITIAL;
    private MinioClient minioClient;

    protected HiveS3DataLake(String bucketName, S s3server, HiveHadoop hiveHadoop)
    {
        this.bucketName = requireNonNull(bucketName, "bucketName is null");
        this.s3server = closer.register(s3server);
        this.hiveHadoop = closer.register(hiveHadoop);
    }

    public T start()
    {
        checkState(state == State.INITIAL, "Already started: %s", state);
        state = State.STARTING;
        s3server.start();
        hiveHadoop.start();
        minioClient = closer.register(s3server.createMinioClient());
        s3server.createBucket(bucketName);
        state = State.STARTED;

        return (T) this;
    }

    public void stop()
            throws Exception
    {
        closer.close();
        state = State.STOPPED;
    }

    @ResourcePresence
    public boolean isNotStopped()
    {
        return state != State.STOPPED;
    }

    public MinioClient getMinioClient()
    {
        checkState(state == State.STARTED, "Can't provide client when S3 server state is: %s", state);
        return minioClient;
    }

    public void copyResources(String resourcePath, String target)
    {
        s3server.copyResources(resourcePath, bucketName, target);
    }

    public void writeFile(byte[] contents, String target)
    {
        s3server.writeFile(contents, bucketName, target);
    }

    public List<String> listFiles(String targetDirectory)
    {
        return getMinioClient().listObjects(getBucketName(), targetDirectory);
    }

    public S getS3Server()
    {
        return s3server;
    }

    public HiveHadoop getHiveHadoop()
    {
        return hiveHadoop;
    }

    public String getBucketName()
    {
        return bucketName;
    }

    @Override
    public void close()
            throws Exception
    {
        stop();
    }

    private enum State
    {
        INITIAL,
        STARTING,
        STARTED,
        STOPPED,
    }
}
