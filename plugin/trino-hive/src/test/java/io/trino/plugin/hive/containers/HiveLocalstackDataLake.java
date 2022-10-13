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

import com.google.common.collect.ImmutableMap;
import io.trino.testing.containers.Localstack;
import org.testcontainers.containers.Network;

import java.util.Map;

import static io.trino.testing.containers.TestContainers.getPathFromClassPathResource;
import static org.testcontainers.containers.Network.newNetwork;

public class HiveLocalstackDataLake
        extends HiveS3DataLake<HiveLocalstackDataLake, Localstack>
{
    public HiveLocalstackDataLake(String bucketName)
    {
        this(bucketName, HiveHadoop.DEFAULT_IMAGE);
    }

    public HiveLocalstackDataLake(String bucketName, String hiveHadoopImage)
    {
        this(bucketName, ImmutableMap.of("/etc/hadoop/conf/core-site.xml", getPathFromClassPathResource("hive_localstack_datalake/hive-core-site.xml")), hiveHadoopImage);
    }

    public HiveLocalstackDataLake(String bucketName, Map<String, String> hiveHadoopFilesToMount, String hiveHadoopImage)
    {
        this(bucketName, hiveHadoopFilesToMount, hiveHadoopImage, newNetwork());
    }

    private HiveLocalstackDataLake(String bucketName, Map<String, String> hiveHadoopFilesToMount, String hiveHadoopImage, Network network)
    {
        super(
                bucketName,
                Localstack.builder()
                .withNetwork(network)
                .build(),
                HiveHadoop.builder()
                .withImage(hiveHadoopImage)
                .withNetwork(network)
                .withFilesToMount(hiveHadoopFilesToMount)
                .build());
    }
}
