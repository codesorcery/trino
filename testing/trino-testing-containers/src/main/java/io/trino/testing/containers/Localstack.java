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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import io.airlift.log.Logger;
import org.testcontainers.containers.Network;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class Localstack
        extends BaseTestContainer
        implements S3Server
{
    private static final Logger log = Logger.get(Localstack.class);

    public static final String DEFAULT_IMAGE = "localstack/localstack:1.4.0";
    public static final String DEFAULT_HOST_NAME = "localstack";

    public static final int LOCALSTACK_API_PORT = 4566;

    public static Builder builder()
    {
        return new Builder();
    }

    private Localstack(
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
    }

    @Override
    public void start()
    {
        super.start();
        log.info("Localstack container started with address for api: http://%s", getApiEndpoint());
    }

    @Override
    public HostAndPort getApiEndpoint()
    {
        return getMappedHostAndPortForExposedPort(LOCALSTACK_API_PORT);
    }

    public static class Builder
            extends BaseTestContainer.Builder<Localstack.Builder, Localstack>
    {
        private Builder()
        {
            this.image = DEFAULT_IMAGE;
            this.hostName = DEFAULT_HOST_NAME;
            this.exposePorts = ImmutableSet.of(LOCALSTACK_API_PORT);
            this.envVars = ImmutableMap.<String, String>builder()
                    .put("AWS_ACCESS_KEY_ID", ACCESS_KEY)
                    .put("AWS_SECRET_ACCESS_KEY", SECRET_KEY)
                    .put("AWS_DEFAULT_REGION", REGION)
                    .buildOrThrow();
        }

        @Override
        public Localstack build()
        {
            return new Localstack(image, hostName, exposePorts, filesToMount, envVars, network, startupRetryLimit);
        }
    }
}
