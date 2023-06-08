
#
# Setup Stage: install apps
#
# This is a dedicated stage so that donwload archives don't end up on 
# production image and consume unnecessary space.
#

FROM ubuntu:22.04 as setup

ENV IB_GATEWAY_VERSION=10.19.1j
ENV IB_GATEWAY_RELEASE_CHANNEL=stable
ENV IBC_VERSION=3.15.2

# Prepare system
RUN apt-get update -y
RUN apt-get install --no-install-recommends --yes \
  curl \
  ca-certificates \
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

ENV IB_GATEWAY_VERSION=10.19.1j

WORKDIR /root

# Prepare system
RUN apt-get update -y &&  DEBIAN_FRONTEND=noninteractive apt-get install -y tzdata

RUN apt-get install --no-install-recommends --yes \
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
  dos2unix

# Copy files
COPY --from=setup /root/ .
RUN chmod a+x /root/scripts/*.sh
COPY --from=setup /usr/local/i4j_jres/ /usr/local/i4j_jres

# IBC env vars
ENV TWS_MAJOR_VRSN ${IB_GATEWAY_VERSION}
ENV TWS_PATH /root/Jts
ENV IBC_PATH /root/ibc
ENV IBC_INI /root/ibc/config.ini
ENV TWOFA_TIMEOUT_ACTION exit

# TBOT: Install Python 3.9 and venv
RUN apt-get install -y software-properties-common && \
    add-apt-repository -y ppa:deadsnakes/ppa

# TBOT: Set python3.9 as a default python3
# TBOT: Set python3.9 as the default python3
RUN apt-get install -y python3.9 python3.9-venv python3.9-dev python3.9-distutils python3-pip && \
    update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.9 1

# TBOT: Create a non-root user and set its home directory
RUN useradd -m tbot

# TBOT: Set the working directory to the user's home directory
WORKDIR /home/tbot

# TBOT: copy environment variables
COPY ./tbot/tbot_run.sh /home/tbot/tbot_run.sh
RUN chmod a+x /home/tbot/tbot_run.sh

# TBOT: Copy SSH keys into the container (if they exist)
RUN mkdir -p /home/tbot/.ssh && \
    chown tbot:tbot /home/tbot/.ssh && \
    (cp ./tbot/id_rsa* /home/tbot/.ssh/ || true)

# TBOT: Set permissions if keys exist
RUN if [ -f /home/tbot/.ssh/id_rsa ]; then \
        chmod 600 /home/tbot/.ssh/id_rsa && \
        chmod 644 /home/tbot/.ssh/id_rsa.pub && \
        ssh-keyscan github.com > /home/tbot/.ssh/known_hosts && \
        git config --global user.email "you@example.com" && \
        git config --global user.name "Your Name" && \
        git config --global core.sshCommand "ssh -i /home/tbot/.ssh/id_rsa -o StrictHostKeyChecking=no"; \
    fi

# TBOT: Install common python libraries for Tradingboat globally
RUN pip install libtmux loguru

# TBOT: create and activate the virtual environment
RUN python3.9 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN . /opt/venv/bin/activate

RUN git clone https://github.com/PlusGenie/tbot-tradingboat.git /home/tbot/develop/github/tbot-tradingboat
WORKDIR /home/tbot/develop/github/tbot-tradingboat
# Install dependencies in the virtual environment
RUN pip install -r requirements.txt
RUN pip install -e .
RUN chmod a+x /home/tbot/develop/github/tbot-tradingboat/tbottmux/*.sh

RUN git clone https://github.com/PlusGenie/tradingview-webhooks-bot.git /home/tbot/develop/github/tradingview-webhooks-bot
WORKDIR /home/tbot/develop/github/tradingview-webhooks-bot/src
# Install dependencies in the virtual environment
RUN pip install -r requirements.txt
# TBOT: Set the default directory
WORKDIR /home/tbot/develop/github/tbot-tradingboat

# Start run script
RUN dos2unix /home/tbot/tbot_run.sh
CMD ["/home/tbot/tbot_run.sh"]
