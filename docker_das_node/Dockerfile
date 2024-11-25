FROM ubuntu:22.04

ARG BASE_DIR="/opt"
ARG TMP_DIR="/tmp"

ARG ATTENTION_BROKER_DIR="${BASE_DIR}/das-attention-broker"
ARG DATA_DIR="${BASE_DIR}/data"
ARG GRPC_DIR="${BASE_DIR}/grpc"
ARG PROTO_DIR="${BASE_DIR}/proto"
ARG BAZEL_DIR="${BASE_DIR}/bazel"
ARG THIRDPARTY="${BASE_DIR}/3rd-party"
# ARG AB_COMMIT="2a25871028268b0b9518d2da2a9d9f29ecfcbb4d"
ARG AB_COMMIT="senna-agi-meetup-stable"

RUN mkdir -p ${DATA_DIR}
RUN mkdir -p ${GRPC_DIR}
RUN mkdir -p ${BAZEL_DIR}
RUN mkdir -p ${PROTO_DIR}
RUN mkdir -p ${THIRDPARTY}
VOLUME ${ATTENTION_BROKER_DIR}

RUN apt-get update -y
RUN apt-get install -y git

RUN cd ${GRPC_DIR} &&\
    git clone https://github.com/grpc/grpc &&\
    cd grpc &&\
    git submodule update --init

RUN apt-get install -y build-essential autoconf libtool pkg-config curl gcc protobuf-compiler libmbedtls14 libmbedtls-dev libevent-dev libssl-dev


################################################################################
# To be removed when AtomDB is properly integrated
# Redis client
RUN apt-get install -y cmake libevent-dev libssl-dev pkg-config cmake-data git

WORKDIR ${BASE_DIR}
RUN git clone https://github.com/singnet/das-attention-broker.git
WORKDIR ${ATTENTION_BROKER_DIR}
RUN git checkout ${AB_COMMIT}
RUN git pull
RUN git pull

# RUN sed -i '24i\AtomDBSingleton::init();' src/main/query_engine_main.cc
# RUN cat src/main/query_engine_main.cc

RUN cp assets/hiredis-cluster.tgz /tmp

RUN cd /tmp &&\
    tar xzf hiredis-cluster.tgz &&\
    cd hiredis-cluster &&\
    mkdir build &&\
    cd build &&\
    cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -DENABLE_SSL=ON ..&&\
    make &&\
    make install &&\
    echo "/usr/local/lib" > /etc/ld.so.conf.d/local.conf &&\
    ldconfig
# MongoDB client
WORKDIR ${ATTENTION_BROKER_DIR}

RUN cp assets/mongo-cxx-driver-r3.11.0.tar.gz /tmp
WORKDIR ${TMP_DIR}
RUN tar xzvf mongo-cxx-driver-r3.11.0.tar.gz
WORKDIR ${TMP_DIR}/mongo-cxx-driver-r3.11.0/build
RUN cmake .. -DCMAKE_BUILD_TYPE=Release -DMONGOCXX_OVERRIDE_DEFAULT_INSTALL_PREFIX=OFF &&\
    cmake --build . &&\
    cmake --build . --target install
RUN pwd &&\
    ln -s /usr/local/include/bsoncxx/v_noabi/bsoncxx/* /usr/local/include/bsoncxx &&\
    ln -s /usr/local/include/bsoncxx/v_noabi/bsoncxx/third_party/mnmlstc/core/ /usr/local/include/core &&\
    ln -s /usr/local/include/mongocxx/v_noabi/mongocxx/* /usr/local/include/mongocxx/ &&\
    ldconfig

################################################################################
WORKDIR ${ATTENTION_BROKER_DIR}

RUN cp assets/3rd-party.tgz ${THIRDPARTY}
RUN cd ${THIRDPARTY} &&\
    tar xzvf 3rd-party.tgz &&\
    rm -f 3rd-party.tgz &&\
    mkdir -p ${ATTENTION_BROKER_DIR}/src/3rd-party &&\
    ln -s ${THIRDPARTY} ${ATTENTION_BROKER_DIR}/src/3rd-party &&\
    mv bazelisk ${BAZEL_DIR}

ENV CPLUS_INCLUDE_PATH="/opt/3rd-party/mbedcrypto/include/"

ENV CC=/usr/bin/gcc
RUN ln -s ${BAZEL_DIR}/bazelisk /usr/bin/bazel
WORKDIR ${ATTENTION_BROKER_DIR}
RUN git pull
RUN mkdir bin
WORKDIR ${ATTENTION_BROKER_DIR}/src
RUN ../scripts/bazel_build.sh

ADD https://raw.githubusercontent.com/singnet/das-query-engine/master/proto/attention_broker.proto ${PROTO_DIR}
ADD https://raw.githubusercontent.com/singnet/das-query-engine/master/proto/common.proto ${PROTO_DIR}
ADD https://raw.githubusercontent.com/singnet/das-query-engine/master/proto/echo.proto ${PROTO_DIR}
EXPOSE 35700
WORKDIR /opt/das-attention-broker

