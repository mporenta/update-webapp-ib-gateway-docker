FROM ubuntu:25.04 AS setup

ENV IB_GATEWAY_VERSION=10.37.1k
ENV IB_GATEWAY_RELEASE_CHANNEL=latest
ENV IBC_VERSION=3.23.0
ENV DEBIAN_FRONTEND=noninteractive
RUN ln -fs /usr/share/zoneinfo/UTC /etc/localtime && echo "UTC" > /etc/timezone
# Prepare system
RUN apt-get update -y
RUN apt-get install --no-install-recommends --yes \
    curl \
    tzdata \
    ca-certificates \
    unzip

WORKDIR /tmp/setup
RUN echo 'https://github.com/UnusualAlpha/ib-gateway-docker/releases/download/ibgateway-'${IB_GATEWAY_RELEASE_CHANNEL}'%40'${IB_GATEWAY_VERSION}'/ibgateway-'${IB_GATEWAY_VERSION}'-standalone-linux-x64.sh' 
# Install IB Gateway
# Use this instead of "RUN curl .." to install a local file: 
#https://github.com/UnusualAlpha/ib-gateway-docker/releases/download/ibgateway-stable%4010.37.1k/ibgateway-10.37.1k-standalone-linux-x64.sh
#https://github.com/UnusualAlpha/ib-gateway-docker/releases/download/ibgateway-latest%4010.37.1k/ibgateway-10.37.1k-standalone-linux-x64.sh
#COPY ibgateway-10.37.1k-standalone-linux-x64.sh .
RUN curl -sSL https://github.com/UnusualAlpha/ib-gateway-docker/releases/download/ibgateway-stable%4010.37.1k/ibgateway-10.37.1k-standalone-linux-x64.sh \
    --output ibgateway-10.37.1k-standalone-linux-x64.sh
RUN curl -sSL https://github.com/UnusualAlpha/ib-gateway-docker/releases/download/ibgateway-stable%4010.37.1k/ibgateway-10.37.1k-standalone-linux-x64.sh.sha256 \
    --output ibgateway-10.37.1k-standalone-linux-x64.sh.sha256
RUN sha256sum --check ./ibgateway-10.37.1k-standalone-linux-x64.sh.sha256
RUN chmod a+x ./ibgateway-10.37.1k-standalone-linux-x64.sh
RUN ./ibgateway-10.37.1k-standalone-linux-x64.sh -q -dir /root/Jts/ibgateway/${IB_GATEWAY_VERSION}
COPY ./config/ibgateway/jts.ini /root/Jts/jts.ini

# Install IBC
RUN curl -sSL https://github.com/IbcAlpha/IBC/releases/download/${IBC_VERSION}/IBCLinux-${IBC_VERSION}.zip --output IBCLinux-${IBC_VERSION}.zip
RUN mkdir /root/ibc
RUN unzip ./IBCLinux-${IBC_VERSION}.zip -d /root/ibc
RUN chmod -R u+x /root/ibc/*.sh 
RUN chmod -R u+x /root/ibc/scripts/*.sh
COPY ./config/ibc/config.ini.tmpl /root/ibc/config.ini.tmpl

# Copy scripts
COPY ./scripts /root/scripts

#
# Build Stage: build production image
#

FROM ubuntu:22.04

ENV IB_GATEWAY_VERSION=10.37.1k
ENV DEBIAN_FRONTEND=noninteractive

WORKDIR /root

# Prepare system with additional packages for ZeroTier
RUN apt-get update -y && apt-get install --no-install-recommends --yes \
    gettext \
    xvfb \
    libxslt-dev \
    libxrender1 \
    libxtst6 \
    libxi6 \
    libgtk2.0-bin \
    socat \
    x11vnc \
    curl \
    gnupg \
    lsb-release \
    net-tools \
    iproute2 \
    procps \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install ZeroTier following official documentation
RUN curl -s https://install.zerotier.com | bash

# Copy files from setup stage
COPY --from=setup /root/ .
RUN chmod a+x /root/scripts/*.sh
COPY --from=setup /usr/local/i4j_jres/ /usr/local/i4j_jres

# Create the ZeroTier startup script
RUN echo '#!/bin/bash' > /root/start-container.sh && \
    echo 'set -e' >> /root/start-container.sh && \
    echo 'echo "=== Starting IB Gateway with ZeroTier ==="' >> /root/start-container.sh && \
    echo 'echo "Following ZeroTier official Docker documentation: https://docs.zerotier.com/docker/"' >> /root/start-container.sh && \
    echo 'echo "Starting ZeroTier daemon with: /usr/sbin/zerotier-one -d"' >> /root/start-container.sh && \
    echo '/usr/sbin/zerotier-one -d' >> /root/start-container.sh && \
    echo 'sleep 5' >> /root/start-container.sh && \
    echo 'if ! pgrep -x "zerotier-one" > /dev/null; then' >> /root/start-container.sh && \
    echo '    echo "ERROR: ZeroTier failed to start"' >> /root/start-container.sh && \
    echo '    exit 1' >> /root/start-container.sh && \
    echo 'fi' >> /root/start-container.sh && \
    echo 'echo "ZeroTier daemon started successfully"' >> /root/start-container.sh && \
    echo 'if [ -n "$ZEROTIER_NETWORK_ID" ]; then' >> /root/start-container.sh && \
    echo '    echo "Joining ZeroTier network: $ZEROTIER_NETWORK_ID"' >> /root/start-container.sh && \
    echo '    /usr/sbin/zerotier-cli join "$ZEROTIER_NETWORK_ID"' >> /root/start-container.sh && \
    echo '    echo "Waiting for network assignment..."' >> /root/start-container.sh && \
    echo '    timeout=60' >> /root/start-container.sh && \
    echo '    while [ $timeout -gt 0 ]; do' >> /root/start-container.sh && \
    echo '        NETWORK_STATUS=$(/usr/sbin/zerotier-cli listnetworks | grep "$ZEROTIER_NETWORK_ID" || echo "")' >> /root/start-container.sh && \
    echo '        if [ -n "$NETWORK_STATUS" ]; then' >> /root/start-container.sh && \
    echo '            echo "Network joined successfully:"' >> /root/start-container.sh && \
    echo '            /usr/sbin/zerotier-cli listnetworks' >> /root/start-container.sh && \
    echo '            break' >> /root/start-container.sh && \
    echo '        fi' >> /root/start-container.sh && \
    echo '        sleep 2' >> /root/start-container.sh && \
    echo '        timeout=$((timeout-2))' >> /root/start-container.sh && \
    echo '    done' >> /root/start-container.sh && \
    echo '    ASSIGNED_IP=$(/usr/sbin/zerotier-cli listnetworks | grep "$ZEROTIER_NETWORK_ID" | awk "{print \$9}" | cut -d"/" -f1)' >> /root/start-container.sh && \
    echo '    if [ -n "$ASSIGNED_IP" ] && [ "$ASSIGNED_IP" != "-" ]; then' >> /root/start-container.sh && \
    echo '        echo "Assigned ZeroTier IP: $ASSIGNED_IP"' >> /root/start-container.sh && \
    echo '        echo "Configure your Python client to connect to: $ASSIGNED_IP:4002"' >> /root/start-container.sh && \
    echo '    else' >> /root/start-container.sh && \
    echo '        echo "IP not yet assigned. Authorize this device in ZeroTier Central."' >> /root/start-container.sh && \
    echo '    fi' >> /root/start-container.sh && \
    echo 'else' >> /root/start-container.sh && \
    echo '    echo "ZEROTIER_NETWORK_ID not provided, skipping network join"' >> /root/start-container.sh && \
    echo 'fi' >> /root/start-container.sh && \
    echo 'echo "=== ZeroTier setup complete ==="' >> /root/start-container.sh && \
    echo 'echo "=== Starting IB Gateway ==="' >> /root/start-container.sh && \
    echo 'exec /root/scripts/run.sh' >> /root/start-container.sh

RUN chmod +x /root/start-container.sh

# IBC environment variables
ENV TWS_MAJOR_VRSN=${IB_GATEWAY_VERSION} \
    TWS_PATH=/root/Jts \
    IBC_PATH=/root/ibc \
    IBC_INI=/root/ibc/config.ini \
    TWOFA_TIMEOUT_ACTION=exit


# Use the startup script app/ib-gateway-docker/latest/requirements.txt
CMD ["/bin/bash", "/root/start-container.sh"]