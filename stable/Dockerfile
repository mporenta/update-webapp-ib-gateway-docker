
#
# Setup Stage: install apps
#
# This is a dedicated stage so that donwload archives don't end up on 
# production image and consume unnecessary space.
#

FROM ubuntu:22.04 AS setup


ENV IB_GATEWAY_VERSION=10.38.1c
ENV IB_GATEWAY_RELEASE_CHANNEL=latest
ENV IBC_VERSION=3.23.0


# Prepare system
RUN apt-get update -y
RUN apt-get install --no-install-recommends --yes \
  curl \
  ca-certificates \
  dos2unix \
  unzip

WORKDIR /tmp/setup

# Install IB Gateway
# Use this instead of "RUN curl .." to install a local file:
#COPY ibgateway-${IB_GATEWAY_VERSION}-standalone-linux-x64.sh .
RUN curl -sSL https://github.com/UnusualAlpha/ib-gateway-docker/releases/download/ibgateway-${IB_GATEWAY_RELEASE_CHANNEL}%40${IB_GATEWAY_VERSION}/ibgateway-${IB_GATEWAY_VERSION}-standalone-linux-x64.sh \
  --output ibgateway-${IB_GATEWAY_VERSION}-standalone-linux-x64.sh
RUN curl -sSL https://github.com/UnusualAlpha/ib-gateway-docker/releases/download/ibgateway-${IB_GATEWAY_RELEASE_CHANNEL}%40${IB_GATEWAY_VERSION}/ibgateway-${IB_GATEWAY_VERSION}-standalone-linux-x64.sh.sha256 \
  --output ibgateway-${IB_GATEWAY_VERSION}-standalone-linux-x64.sh.sha256
RUN sha256sum --check ./ibgateway-${IB_GATEWAY_VERSION}-standalone-linux-x64.sh.sha256
RUN chmod a+x ./ibgateway-${IB_GATEWAY_VERSION}-standalone-linux-x64.sh
RUN ./ibgateway-${IB_GATEWAY_VERSION}-standalone-linux-x64.sh -q -dir /root/Jts/ibgateway/${IB_GATEWAY_VERSION}
COPY ./config/ibgateway/jts.ini /root/Jts/jts.ini
# make sure script line endings are LF, not CRLF


# Install IBC
RUN curl -sSL https://github.com/IbcAlpha/IBC/releases/download/${IBC_VERSION}/IBCLinux-${IBC_VERSION}.zip \
     --output IBCLinux-${IBC_VERSION}.zip

RUN mkdir /root/ibc \
 && unzip ./IBCLinux-${IBC_VERSION}.zip -d /root/ibc \
 && dos2unix /root/ibc/*.sh /root/ibc/scripts/*.sh \
 && chmod -R u+x /root/ibc/*.sh /root/ibc/scripts/*.sh

COPY ./config/ibc/config.ini.tmpl /root/ibc/config.ini.tmpl

# Copy scripts
COPY ./scripts /root/scripts

#
# Build Stage: build production image
#

#
# Build Stage: build production image
#

FROM ubuntu:22.04

ENV IB_GATEWAY_VERSION=10.38.1c
ENV IB_GATEWAY_RELEASE_CHANNEL=latest
ENV IBC_VERSION=3.23.0

WORKDIR /root
RUN apt-get update -y &&  DEBIAN_FRONTEND=noninteractive apt-get install -y tzdata
# Install all runtime dependencies and clean up
RUN apt-get install --no-install-recommends --yes \
  build-essential \
  autoconf \
  automake \
  libtool \
  libffi-dev \
  libssl-dev \
  python3.11-dev \
  python3.11-venv \
  python3-pip \
  gettext \
  xvfb \
  libxslt-dev \
  libxrender1 \
  libxtst6 \
  libxi6 \
  libgtk2.0-bin \
  socat \
  x11vnc \
  git \
  redis-server \
  wget \
  unzip \
  tmux \
  openssh-client \
  net-tools \
  vim \
  dos2unix \
  ca-certificates && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*



# Copy files from setup stage
COPY --from=setup /root/ .  
COPY --from=setup /usr/local/i4j_jres/ /usr/local/i4j_jres
RUN chmod a+x /root/scripts/*.sh

# IBC env vars
ENV TWS_MAJOR_VRSN=${IB_GATEWAY_VERSION}
ENV TWS_PATH=/root/Jts
ENV IBC_PATH=/root/ibc
ENV IBC_INI=/root/ibc/config.ini
ENV TWOFA_TIMEOUT_ACTION=exit



# TBOT: Set python3.11 as default
RUN apt-get install -y python3.11 python3.11-venv python3.11-dev python3.11-distutils python3-pip && \
    update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1
# Download, build, install TA-Lib C library v0.6.4 (shared + static libs + headers)
RUN wget https://github.com/ta-lib/ta-lib/releases/download/v0.6.4/ta-lib-0.6.4-src.tar.gz -O ta-lib-0.6.4.tar.gz && \
    tar -xzf ta-lib-0.6.4.tar.gz && \
    cd ta-lib-0.6.4 && \
    ./configure --prefix=/usr && \         
    make && \                               
    make install && \                     
    cd .. && \
    rm -rf ta-lib-0.6.4 ta-lib-0.6.4.tar.gz
# ─────────────────────────────────────
# TBOT: Create non-root user
RUN useradd -m tbot
WORKDIR /home/tbot

# TBOT: copy run script
COPY ./tbot/tbot_run.sh /home/tbot/tbot_run.sh
RUN chmod a+x /home/tbot/tbot_run.sh

# TBOT: Install global Python libs
RUN pip install libtmux loguru

# TBOT: create and activate the virtualenv
RUN python3.11 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN . /opt/venv/bin/activate

COPY squeeze_pro.py /opt/venv/lib/python3.11/site-packages/pandas_ta/momentum/squeeze_pro.py

# After copying in tbot-tradingboat:
COPY ./tbot-tradingboat /home/tbot/develop/github/tbot-tradingboat
WORKDIR /home/tbot/develop/github/tbot-tradingboat
# normalize line endings for both shell and python scripts, then install
RUN dos2unix tbottmux/*.sh tbottmux/*.py && \
    pip install -r requirements.txt && \
    pip install -e . && \
    pip install TA-Lib && \
    chmod a+x tbottmux/*.sh

# ─────────────────────────────────────
# After copying in tradingview-webhooks-bot:
COPY ./tradingview-webhooks-bot /home/tbot/develop/github/tradingview-webhooks-bot
WORKDIR /home/tbot/develop/github/tradingview-webhooks-bot/src
# normalize line endings for the launcher
RUN dos2unix tvwb.py && \
    pip install -r requirements.txt && \
    pip install TA-Lib
    

# Reset to the default working directory
WORKDIR /home/tbot/develop/github/tbot-tradingboat
# ──────────────────────────────────────────────────────────────────────────────

# copy remaining helper scripts
COPY ./scripts/fork_ports_delayed.sh /root/scripts/fork_ports_delayed.sh
COPY ./scripts/run_x11_vnc.sh   /root/scripts/run_x11_vnc.sh
RUN dos2unix /root/scripts/*.sh && chmod a+x /root/scripts/*.sh

# Start run script
RUN dos2unix /home/tbot/tbot_run.sh && chmod a+x /home/tbot/tbot_run.sh
EXPOSE 5000
CMD ["/home/tbot/tbot_run.sh"]
