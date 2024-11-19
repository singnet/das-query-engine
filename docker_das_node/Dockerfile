# FROM das-node-builder
# ARG BASE_DIR="/opt"
# ARG ATTENTION_BROKER_DIR="${BASE_DIR}/das-attention-broker"
# ARG THIRDPARTY="${ATTENTION_BROKER_DIR}/assets"
#
# WORKDIR /opt/hyperon_das_node/src
# RUN ../scripts/bazel_build.sh
#
#
# WORKDIR /opt
# RUN git clone https://github.com/singnet/das-attention-broker.git
# WORKDIR /opt/das-attention-broker/
# RUN ls -la
# RUN cd ${THIRDPARTY}\
#     && tar xzvf 3rd-party.tgz\
#     && rm -f 3rd-party.tgz\
#     && mkdir -p ${ATTENTION_BROKER_DIR}/src/3rd-party\
#     && ln -s /opt/3rd-party ${ATTENTION_BROKER_DIR}/src/3rd-party
#
# ENV CPLUS_INCLUDE_PATH="/opt/3rd-party/mbedcrypto/include/"
#
#
# WORKDIR /opt/das-attention-broker/src
# RUN ls -l
# RUN pwd
# # RUN ../scripts/bazel_build.sh
# RUN /opt/bazel/bazelisk build --jobs 6 --noenable_bzlmod :query_broker
# # RUN mv bazel-bin/query_broker ../bin
# WORKDIR /opt/das-attention-broker
#
# CMD .bin/query_broker
#
# # RUN pip install --no-cache-dir hyperon_das_node
# #
# # WORKDIR /opt/hyperon_das_node
# #
# # COPY *.py .
# #
# #
# # # Server image for docker example, exposes server port
# # FROM base AS server
# #
# # EXPOSE 35700
# #
# # ENTRYPOINT ["python", "-i", "das_node_server.py"]



FROM ubuntu:22.04

ARG BASE_DIR="/opt"
ARG TMP_DIR="/tmp"

ARG ATTENTION_BROKER_DIR="${BASE_DIR}/das-attention-broker"
ARG DATA_DIR="${BASE_DIR}/data"
ARG GRPC_DIR="${BASE_DIR}/grpc"
ARG PROTO_DIR="${BASE_DIR}/proto"
ARG BAZEL_DIR="${BASE_DIR}/bazel"
ARG THIRDPARTY="${BASE_DIR}/3rd-party"

# RUN mkdir -p ${ATTENTION_BROKER_DIR}
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
# RUN apt-get install -y autoconf
# RUN apt-get install -y libtool
# RUN apt-get install -y pkg-config
# RUN apt-get install -y curl
# RUN apt-get install -y gcc
# RUN apt-get install -y protobuf-compiler
# #RUN apt-get install -y libmbedcrypto7
# RUN apt-get install -y libmbedtls14
# RUN apt-get install -y libmbedtls-dev
# RUN apt-get install -y libevent-dev
# RUN apt-get install -y libssl-dev

################################################################################
# To be removed when AtomDB is properly integrated
# Redis client
RUN apt-get install -y cmake libevent-dev libssl-dev pkg-config cmake-data git

WORKDIR ${BASE_DIR}
RUN git clone https://github.com/singnet/das-attention-broker.git
WORKDIR ${ATTENTION_BROKER_DIR}

RUN sed -i '24i\AtomDBSingleton::init();' src/main/query_engine_main.cc
RUN cat src/main/query_engine_main.cc

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

# RUN cp assets/mongo-cxx-driver-r3.11.0.tar.gz /tmp &&\
#     cd /tmp && \
#     tar xzvf mongo-cxx-driver-r3.11.0.tar.gz &&\
#     cd /tmp/mongo-cxx-driver-r3.11.0/build/ &&\
#     cmake .. -DCMAKE_BUILD_TYPE=Release -DMONGOCXX_OVERRIDE_DEFAULT_INSTALL_PREFIX=OFF &&\
#     cmake --build .
RUN cp assets/mongo-cxx-driver-r3.11.0.tar.gz /tmp
WORKDIR ${TMP_DIR}
RUN tar xzvf mongo-cxx-driver-r3.11.0.tar.gz
WORKDIR ${TMP_DIR}/mongo-cxx-driver-r3.11.0/build
RUN cmake .. -DCMAKE_BUILD_TYPE=Release -DMONGOCXX_OVERRIDE_DEFAULT_INSTALL_PREFIX=OFF &&\
    cmake --build . &&\
    cmake --build . --target install
RUN pwd &&\
    # ls -a   &&\
    # ls /usr/local/include/mongocxx/v_noabi &&\
    # ls -a .. &&\
    # mv install/include/* /usr/local/include &&\
    ln -s /usr/local/include/bsoncxx/v_noabi/bsoncxx/* /usr/local/include/bsoncxx &&\
    ln -s /usr/local/include/bsoncxx/v_noabi/bsoncxx/third_party/mnmlstc/core/ /usr/local/include/core &&\
    ln -s /usr/local/include/mongocxx/v_noabi/mongocxx/* /usr/local/include/mongocxx/ &&\
    # mv install/lib/* /usr/local/lib &&\
    ldconfig
# RUN cmake --build . --target install &&\
#     mv install/include/* /usr/local/include &&\
#     ln -s /usr/local/include/bsoncxx/v_noabi/bsoncxx/* /usr/local/include/bsoncxx &&\
#     ln -s /usr/local/include/bsoncxx/v_noabi/bsoncxx/third_party/mnmlstc/core/ /usr/local/include/core &&\
#     ln -s /usr/local/include/mongocxx/v_noabi/mongocxx/* /usr/local/include/mongocxx/ &&\
#     mv install/lib/* /usr/local/lib &&\
#     ldconfig
################################################################################
WORKDIR ${ATTENTION_BROKER_DIR}
RUN mkdir bin

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
# RUN cd ${GRPC_DIR}/grpc &&\
#     ${BAZEL_DIR}/bazelisk build :all

# RUN cd ${GRPC_DIR}/grpc &&\
#     ${BAZEL_DIR}/bazelisk build --jobs 6 --noenable_bzlmod :query_broker
# RUN ${BAZEL_DIR}/bazelisk build --jobs 6 --noenable_bzlmod :query_broker
WORKDIR ${ATTENTION_BROKER_DIR}
RUN rm -rf  bin
RUN mkdir bin
WORKDIR ${ATTENTION_BROKER_DIR}/src
# RUN ${BAZEL_DIR}/bazelisk build --jobs 6 --noenable_bzlmod :query_broker
# RUN mv bazel-bin/query_broker ../query_broker
RUN ../scripts/bazel_build.sh
# RUN cp bazel-bin/query_broker ../
# build --jobs 6 --noenable_bzlmod :query_broker
ADD https://raw.githubusercontent.com/singnet/das-query-engine/master/proto/attention_broker.proto ${PROTO_DIR}
ADD https://raw.githubusercontent.com/singnet/das-query-engine/master/proto/common.proto ${PROTO_DIR}
ADD https://raw.githubusercontent.com/singnet/das-query-engine/master/proto/echo.proto ${PROTO_DIR}
EXPOSE 35700
WORKDIR /opt/das-attention-broker

RUN ls -la
# RUN ls -la bin/
# RUN ls  /opt/bazel
# RUN ls  /opt
