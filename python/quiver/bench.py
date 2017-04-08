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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from __future__ import with_statement

import argparse as _argparse
import os as _os
import plano as _plano
import shlex as _shlex
import subprocess as _subprocess
import time as _time
import traceback as _traceback

from .common import *
from .common import __version__

_description = """
Benchmark message sender, receiver, and server combinations.

'quiver-bench' is one of the Quiver tools for testing the performance
of message servers and APIs.
"""

_epilog = """
The --include-* and --exclude-* arguments take comma-separated lists
of implementation names.  Use 'quiver-arrow --help' and
'quiver-server --help' to list the available implementations.
"""

class QuiverBenchCommand(Command):
    def __init__(self, home_dir):
        super(QuiverBenchCommand, self).__init__(home_dir)

        self.parser.description = _description.lstrip()
        self.parser.epilog = _epilog.lstrip()

        self.parser.add_argument("--output", metavar="DIR",
                                 help="Save output files to DIR")
        self.parser.add_argument("--include-senders", metavar="IMPLS",
                                 help="Test only senders in IMPLS",
                                 default="all")
        self.parser.add_argument("--include-receivers", metavar="IMPLS",
                                 help="Test only receivers in IMPLS",
                                 default="all")
        self.parser.add_argument("--include-servers", metavar="IMPLS",
                                 help="Test only servers in IMPLS",
                                 default="all")
        self.parser.add_argument("--exclude-senders", metavar="IMPLS",
                                 help="Do not test senders in IMPLS",
                                 default="none")
        self.parser.add_argument("--exclude-receivers", metavar="IMPLS",
                                 help="Do not test receivers in IMPLS",
                                 default="none")
        self.parser.add_argument("--exclude-servers", metavar="IMPLS",
                                 help="Do not test servers in IMPLS",
                                 default="builtin")
        self.parser.add_argument("--client-server", action="store_true",
                                 help="Test only client-server mode")
        self.parser.add_argument("--peer-to-peer", action="store_true",
                                 help="Test only peer-to-peer mode")
        self.parser.add_argument("--mixed-pairs", action="store_true",
                                 help="Test unmatched senders and receivers")

        self.add_common_test_arguments()
        self.add_common_tool_arguments()

    def init(self):
        super(QuiverBenchCommand, self).init()

        self.output_dir = self.args.output

        if self.output_dir is None:
            prefix = _plano.program_name()
            datestamp = _time.strftime('%Y-%m-%d', _time.localtime())

            self.output_dir = "{}-{}".format(prefix, datestamp)

        _plano.remove(self.output_dir)
        _plano.make_dir(self.output_dir)

        self.client_server = True
        self.peer_to_peer = True

        if self.args.client_server:
            self.peer_to_peer = False

        if self.args.peer_to_peer:
            self.client_server = False

        self.mixed_pairs = self.args.mixed_pairs

        self.init_impl_attributes()
        self.init_common_test_attributes()
        self.init_common_tool_attributes()

        _plano.set_message_threshold("warn")

        self.failures = list()

    def init_impl_attributes(self):
        sender_impls = set(ARROW_IMPLS)
        receiver_impls = set(ARROW_IMPLS)
        server_impls = set(SERVER_IMPLS)

        if self.args.include_senders != "all":
            sender_impls = self.parse_arrow_impls(self.args.include_senders)

        if self.args.include_receivers != "all":
            receiver_impls = self.parse_arrow_impls(self.args.include_receivers)

        if self.args.include_servers != "all":
            server_impls = self.parse_server_impls(self.args.include_servers)

        if self.args.exclude_senders != "none":
            sender_impls -= self.parse_arrow_impls(self.args.exclude_senders)

        if self.args.exclude_receivers != "none":
            receiver_impls -= self.parse_arrow_impls(self.args.exclude_receivers)

        if self.args.exclude_servers != "none":
            server_impls -= self.parse_server_impls(self.args.exclude_servers)

        for impl in list(sender_impls):
            file = self.get_arrow_impl_file(impl)

            if not _plano.exists(file):
                _plano.warn("No implementation at '{}'; skipping it", file)
                sender_impls.remove(impl)

        for impl in list(receiver_impls):
            file = self.get_arrow_impl_file(impl)

            if not _plano.exists(file):
                _plano.warn("No implementation at '{}'; skipping it", file)
                receiver_impls.remove(impl)

        for impl in list(server_impls):
            file = self.get_server_impl_file(impl)

            if not _plano.exists(file):
                _plano.warn("No implementation at '{}'; skipping it", file)
                server_impls.remove(impl)

        self.sender_impls = sorted(sender_impls)
        self.receiver_impls = sorted(receiver_impls)
        self.server_impls = sorted(server_impls)

    def parse_arrow_impls(self, value):
        impls = set()

        for name in value.split(","):
            impls.add(self.get_arrow_impl_name(name, name))

        return impls

    def parse_server_impls(self, value):
        impls = set()

        for name in value.split(","):
            impls.add(self.get_server_impl_name(name, name))

        return impls

    def run(self):
        if self.client_server:
            for sender_impl in self.sender_impls:
                for receiver_impl in self.receiver_impls:
                    if not self.mixed_pairs:
                        if sender_impl != receiver_impl:
                            continue

                    if sender_impl in OPENWIRE_ARROW_IMPLS:
                        if receiver_impl not in OPENWIRE_ARROW_IMPLS:
                            continue

                    if receiver_impl in OPENWIRE_ARROW_IMPLS:
                        if sender_impl not in OPENWIRE_ARROW_IMPLS:
                            continue

                    if sender_impl in CORE_PROTOCOL_ARROW_IMPLS:
                        if receiver_impl not in CORE_PROTOCOL_ARROW_IMPLS:
                            continue

                    if receiver_impl in CORE_PROTOCOL_ARROW_IMPLS:
                        if sender_impl not in CORE_PROTOCOL_ARROW_IMPLS:
                            continue

                    for server_impl in self.server_impls:
                        if sender_impl in OPENWIRE_ARROW_IMPLS:
                            if server_impl not in OPENWIRE_SERVER_IMPLS:
                                continue

                        if sender_impl in CORE_PROTOCOL_ARROW_IMPLS:
                            if server_impl not in CORE_PROTOCOL_SERVER_IMPLS:
                                continue

                        self.run_test(sender_impl, server_impl, receiver_impl)

        if self.peer_to_peer:
            for sender_impl in self.sender_impls:
                if sender_impl in OPENWIRE_ARROW_IMPLS:
                    continue

                if sender_impl in CORE_PROTOCOL_ARROW_IMPLS:
                    continue

                for receiver_impl in self.receiver_impls:
                    if not self.mixed_pairs:
                        if sender_impl != receiver_impl:
                            continue

                    if receiver_impl not in PEER_TO_PEER_ARROW_IMPLS:
                        continue

                    self.run_test(sender_impl, None, receiver_impl)

        print("Test failures: {}".format(len(self.failures)))

        for failure in self.failures:
            print(failure) # Need summary

        if len(self.failures) > 0:
            _plano.exit(1)

    def run_test(self, sender_impl, server_impl, receiver_impl):
        peer_to_peer = server_impl is None
        port = _plano.random_port()
        server = None

        if server_impl == "activemq":
            if sender_impl == "activemq-jms" and receiver_impl == "activemq-jms":
                port = 61616
            else:
                port = 5672

        if peer_to_peer:
            summary = "{} -> {} ".format(sender_impl, receiver_impl)
            server_name = "none"
        else:
            summary = "{} -> {} -> {} ".format(sender_impl, server_impl, receiver_impl)
            server_name = server_impl

        test_dir = _plano.join(self.output_dir, sender_impl, server_name, receiver_impl)
        pair_dir = _plano.join(test_dir, "pair")
        server_dir = _plano.join(test_dir, "server")

        pair = _TestPair(pair_dir, sender_impl, receiver_impl, peer_to_peer)

        if not peer_to_peer:
            server = _TestServer(server_dir, server_impl)
            _plano.make_dir(server.output_dir)

        print("{:.<113} ".format(summary), end="")

        _plano.flush()

        if server is not None:
            out = open(server.output_file, "w")

            try:
                server.start(port, out)
            except _Timeout as e:
                self.failures.append(str(e)) # XXX capture the combo

                print("FAILED")

                if self.verbose:
                    if server is not None:
                        server.print_summary()

        try:
            pair.run(port, self.args)

            print("PASSED")
        except KeyboardInterrupt:
            raise
        except _plano.CalledProcessError as e:
            self.failures.append(str(e)) # XXX capture the combo

            print("FAILED")

            if self.verbose:
                pair.print_summary()

                if server is not None:
                    server.print_summary()
        except:
            _traceback.print_exc()
        finally:
            _plano.flush()

            if server is not None:
                server.stop()
                out.close()

        self.report(pair, server)

    def report(self, pair, server):
        pass

class _TestPair(object):
    def __init__(self, output_dir, sender_impl, receiver_impl, peer_to_peer):
        self.output_dir = output_dir
        self.sender_impl = sender_impl
        self.receiver_impl = receiver_impl
        self.peer_to_peer = peer_to_peer

        self.command_file = _plano.join(self.output_dir, "command.txt")
        self.output_file = _plano.join(self.output_dir, "output.txt")
        self.status_file = _plano.join(self.output_dir, "status.txt")

    def run(self, port, args):
        _plano.make_dir(self.output_dir)

        command = [
            "quiver", "//127.0.0.1:{}/q0".format(port),
            "--sender", self.sender_impl,
            "--receiver", self.receiver_impl,
            "--messages", args.messages,
            "--body-size", args.body_size,
            "--credit", args.credit,
            "--timeout", args.timeout,
            "--output", self.output_dir,
        ]

        if self.peer_to_peer:
            command.append("--peer-to-peer")

        _plano.write(self.command_file, "{}\n".format(" ".join(command)))

        with open(self.output_file, "w") as f:
            try:
                _plano.call(command, stdout=f, stderr=f)
            except:
                _plano.write(self.status_file, "FAILED\n")
                raise

        _plano.write(self.status_file, "PASSED\n")

    def print_summary(self):
        print("--- Test command ---")
        print("> {}".format(_plano.read(self.command_file)))
        print("--- Test output ---")

        for line in _plano.read_lines(self.output_file):
            print("> {}".format(line), end="")

        print()

class _TestServer(object):
    def __init__(self, output_dir, impl):
        self.output_dir = output_dir
        self.impl = impl

        self.ready_file = _plano.make_temp_file()
        self.command_file = _plano.join(self.output_dir, "command.txt")
        self.output_file = _plano.join(self.output_dir, "output.txt")
        self.status_file = _plano.join(self.output_dir, "status.txt")

        self.proc = None

    def start(self, port, out):
        assert self.proc is None

        _plano.make_dir(self.output_dir)

        command = [
            "quiver-server", "//127.0.0.1:{}/q0".format(port),
            "--impl", self.impl,
            "--ready-file", self.ready_file,
            "--verbose",
        ]

        _plano.write(self.command_file, "{}\n".format(" ".join(command)))

        self.proc = _plano.start_process(command, stdout=out, stderr=out)

        for i in range(30):
            if _plano.read(self.ready_file) == "ready\n":
                break

            _plano.sleep(1)
        else:
            raise _Timeout("Timed out waiting for server to be ready")

    def stop(self):
        assert self.proc is not None

        _plano.stop_process(self.proc)

        if self.proc.returncode > 0:
            _plano.write(self.status_file, "FAILED\n")
        else:
            _plano.write(self.status_file, "PASSED\n")

    def print_summary(self):
        print("--- Server command ---")
        print("> {}".format(_plano.read(self.command_file)))
        print("--- Server output ---")

        for line in _plano.read_lines(self.output_file):
            print("> {}".format(line), end="")

        print()

class _Timeout(Exception):
    pass