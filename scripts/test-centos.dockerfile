#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

FROM registry.centos.org/centos/centos:8

RUN dnf -qy update && dnf -q clean all

RUN dnf -qy install epel-release

RUN dnf -qy install gcc-c++ java-11-openjdk-devel make maven nodejs python3-numpy unzip xz

RUN dnf -y install cyrus-sasl-devel cyrus-sasl-md5 cyrus-sasl-plain python3-qpid-proton qpid-proton-c-devel qpid-proton-cpp-devel

COPY . /root/quiver

RUN cd /root/quiver && make install PREFIX=/usr

WORKDIR /root
CMD ["quiver-test"]
