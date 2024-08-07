#
# Setup Stage: install apps
#
# This stage is dedicated to installing applications so that download archives
# don't end up on the production image and consume unnecessary space.
#

FROM ubuntu:22.04 AS setup

# Set IB Gateway and IBC versions
ENV IB_GATEWAY_VERSION=10.23.2a
ENV IB_GATEWAY_RELEASE_CHANNEL=latest
ENV IBC_VERSION=3.17.0

# Prepare the system and install necessary packages
RUN apt-get update -y && \
  apt-get install --no-install-recommends --yes \
  curl \
  ca-certificates \
  unzip && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

WORKDIR /tmp/setup

# Install IB Gateway
RUN curl -sSL https://github.com/UnusualAlpha/ib-gateway-docker/releases/download/ibgateway-${IB_GATEWAY_RELEASE_CHANNEL}%40${IB_GATEWAY_VERSION}/ibgateway-${IB_GATEWAY_VERSION}-standalone-linux-x64.sh \
  --output ibgateway-${IB_GATEWAY_VERSION}-standalone-linux-x64.sh && \
  curl -sSL https://github.com/UnusualAlpha/ib-gateway-docker/releases/download/ibgateway-${IB_GATEWAY_RELEASE_CHANNEL}%40${IB_GATEWAY_VERSION}/ibgateway-${IB_GATEWAY_VERSION}-standalone-linux-x64.sh.sha256 \
  --output ibgateway-${IB_GATEWAY_VERSION}-standalone-linux-x64.sh.sha256 && \
  sha256sum --check ./ibgateway-${IB_GATEWAY_VERSION}-standalone-linux-x64.sh.sha256 && \
  chmod a+x ./ibgateway-${IB_GATEWAY_VERSION}-standalone-linux-x64.sh && \
  ./ibgateway-${IB_GATEWAY_VERSION}-standalone-linux-x64.sh -q -dir /root/Jts/ibgateway/${IB_GATEWAY_VERSION}

COPY ./config/ibgateway/jts.ini /root/Jts/jts.ini

# Install IBC
RUN curl -sSL https://github.com/IbcAlpha/IBC/releases/download/${IBC_VERSION}/IBCLinux-${IBC_VERSION}.zip --output IBCLinux-${IBC_VERSION}.zip && \
  mkdir /root/ibc && \
  unzip ./IBCLinux-${IBC_VERSION}.zip -d /root/ibc && \
  chmod -R u+x /root/ibc/*.sh && \
  chmod -R u+x /root/ibc/scripts/*.sh

COPY ./config/ibc/config.ini.tmpl /root/ibc/config.ini.tmpl

# Copy scripts
COPY ./scripts /root/scripts

#
# Build Stage: build production image
#

FROM ubuntu:22.04 AS production

# Re-declare ARGs to ensure they are available in this stage
ARG TBOT_TRADINGBOAT_GIT_REPO=https://github.com/PlusGenie/tbot-tradingboat.git
ARG TBOT_TRADINGBOAT_GIT_BRANCH=main
ARG TBOT_TVWB_GIT_REPO=https://github.com/mporenta/tradingview-webhooks-bot.git
ARG TBOT_TVWB_GIT_BRANCH=dev2.0

# Set environment variables
ENV IB_GATEWAY_VERSION=10.23.2a
ENV TWS_MAJOR_VRSN=${IB_GATEWAY_VERSION}
ENV TWS_PATH=/root/Jts
ENV IBC_PATH=/root/ibc
ENV IBC_INI=/root/ibc/config.ini
ENV TWOFA_TIMEOUT_ACTION=exit

WORKDIR /root

# Prepare the system and install necessary packages
RUN apt-get update -y && \
  DEBIAN_FRONTEND=noninteractive apt-get install -y tzdata && \
  apt-get install --no-install-recommends --yes \
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
  dos2unix && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

# Copy files from the setup stage
COPY --from=setup /root/ .
RUN chmod a+x /root/scripts/*.sh
COPY --from=setup /usr/local/i4j_jres/ /usr/local/i4j_jres

# Install Python 3.9
RUN apt-get update && \
  apt-get install -y software-properties-common && \
  add-apt-repository -y ppa:deadsnakes/ppa && \
  apt-get update && \
  apt-get install -y python3.9 python3.9-venv python3.9-dev python3.9-distutils python3-pip && \
  update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.9 1 && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

# Create a non-root user and set its home directory
RUN useradd -m tbot

# Set the working directory to the user's home directory
WORKDIR /home/tbot

# Copy environment variables and scripts
COPY ./tbot/tbot_run.sh /home/tbot/tbot_run.sh
RUN chmod a+x /home/tbot/tbot_run.sh

# Verify ARG values with echo statements (for debugging)
RUN echo "TradingBoat Git Repo: $TBOT_TRADINGBOAT_GIT_REPO"
RUN echo "TradingBoat Git Branch: $TBOT_TRADINGBOAT_GIT_BRANCH"
RUN echo "TVWB Git Repo: $TBOT_TVWB_GIT_REPO"
RUN echo "TVWB Git Branch: $TBOT_TVWB_GIT_BRANCH"

# Clone and set up the tbot-tradingboat repository
RUN git clone -b ${TBOT_TRADINGBOAT_GIT_BRANCH} ${TBOT_TRADINGBOAT_GIT_REPO} /home/tbot/develop/github/tbot-tradingboat && \
    cd /home/tbot/develop/github/tbot-tradingboat && \
    git pull origin ${TBOT_TRADINGBOAT_GIT_BRANCH} && \
    pip install -r requirements.txt && \
    pip install -e . && \
    chmod a+x /home/tbot/develop/github/tbot-tradingboat/tbottmux/*.sh

# Clone the specific branch of the tradingview-webhooks-bot repository
RUN git clone -b ${TBOT_TVWB_GIT_BRANCH} --single-branch ${TBOT_TVWB_GIT_REPO} /home/tbot/develop/github/tradingview-webhooks-bot && \
    cd /home/tbot/develop/github/tradingview-webhooks-bot/src && \
    git pull origin ${TBOT_TVWB_GIT_BRANCH} && \
    pip install -r requirements.txt

# Set the default directory
WORKDIR /home/tbot/develop/github/tbot-tradingboat

# Copy scripts
COPY ./scripts/fork_ports_delayed.sh /root/scripts/fork_ports_delayed.sh
COPY ./scripts/run_x11_vnc.sh /root/scripts/run_x11_vnc.sh
RUN dos2unix /root/scripts/*.sh && chmod a+x /root/scripts/*.sh

# Start the run script
CMD ["/home/tbot/tbot_run.sh"]
