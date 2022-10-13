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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import io.airlift.log.Logger;
import org.testcontainers.containers.Network;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class Ceph
        extends BaseTestContainer
        implements S3Server
{
    private static final Logger log = Logger.get(Ceph.class);

    private static final String DEFAULT_IMAGE = "quay.io/ceph/daemon:v7.0.1-stable-7.0-quincy-centos-stream8";
    private static final String DEFAULT_HOST_NAME = "ceph";

    private static final int CEPH_RGW_PORT = 8080;

    public static Builder builder()
    {
        return new Builder();
    }

    private Ceph(
            String image,
            String hostName,
            Set<Integer> exposePorts,
            Map<String, String> filesToMount,
            Map<String, String> envVars,
            Optional<Network> network,
            int retryLimit)
    {
        super(
                image,
                hostName,
                exposePorts,
                filesToMount,
                envVars,
                network,
                retryLimit);
    }

    @Override
    protected void setupContainer()
    {
        super.setupContainer();
        withRunCommand(ImmutableList.of("demo"));
    }

    @Override
    public void start()
    {
        super.start();
        log.info("Ceph container started with address for S3 api: http://%s", getApiEndpoint());
    }

    public HostAndPort getApiEndpoint()
    {
        return getMappedHostAndPortForExposedPort(CEPH_RGW_PORT);
    }

    public static class Builder
            extends BaseTestContainer.Builder<Ceph.Builder, Ceph>
    {
        private Builder()
        {
            this.image = DEFAULT_IMAGE;
            this.hostName = DEFAULT_HOST_NAME;
            this.exposePorts = ImmutableSet.of(CEPH_RGW_PORT);
            this.envVars = ImmutableMap.<String, String>builder()
                    .put("CEPH_DAEMON", "demo")
                    .put("CEPH_DEMO_UID", "ceph")
                    .put("CEPH_DEMO_ACCESS_KEY", ACCESS_KEY)
                    .put("CEPH_DEMO_SECRET_KEY", SECRET_KEY)
                    .put("NETWORK_AUTO_DETECT", "4")
                    .put("RGW_NAME", DEFAULT_HOST_NAME)
                    .buildOrThrow();
        }

        @Override
        public Ceph build()
        {
            return new Ceph(image, hostName, exposePorts, filesToMount, envVars, network, startupRetryLimit);
        }
    }
}
