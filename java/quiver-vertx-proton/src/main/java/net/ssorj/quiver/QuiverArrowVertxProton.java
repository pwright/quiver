/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package net.ssorj.quiver;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.message.Message;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonReceiver;
import io.vertx.proton.ProtonSender;

public class QuiverArrowVertxProton {
    private static final String CLIENT = "client";
    private static final String ACTIVE = "active";
    private static final String RECEIVE = "receive";
    private static final String SEND = "send";

    private static final Accepted ACCEPTED = Accepted.getInstance();

    public static void main(String[] args) {
        try {
            doMain(args);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void doMain(String[] args) throws Exception {
        final String connectionMode = args[0];
        final String channelMode = args[1];
        final String operation = args[2];
        final String id = args[3];
        final String host = args[4];
        final String port = args[5];
        final String path = args[6];
        final int seconds = Integer.parseInt(args[7]);
        final int messages = Integer.parseInt(args[8]);
        final int bodySize = Integer.parseInt(args[9]);
        final int creditWindow = Integer.parseInt(args[10]);
        final int transactionSize = Integer.parseInt(args[11]);

        final String[] flags = args[12].split(",");

        final boolean durable = Arrays.asList(flags).contains("durable");

        if (!CLIENT.equalsIgnoreCase(connectionMode)) {
            throw new RuntimeException("This impl currently supports client mode only");
        }

        if (!ACTIVE.equalsIgnoreCase(channelMode)) {
            throw new RuntimeException("This impl currently supports active mode only");
        }

        if (transactionSize > 0) {
            throw new RuntimeException("This impl doesn't support transactions");
        }

        final boolean sender;

        if (SEND.equalsIgnoreCase(operation)) {
            sender = true;
        } else if (RECEIVE.equalsIgnoreCase(operation)) {
            sender = false;
        } else {
            throw new java.lang.IllegalStateException("Unknown operation: " + operation);
        }

        final int portNumber = Integer.parseInt(port);

        AtomicBoolean stopping = new AtomicBoolean();
        CountDownLatch completionLatch = new CountDownLatch(1);
        Vertx vertx = Vertx.vertx(new VertxOptions().setPreferNativeTransport(true));

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(host, portNumber, res -> {
                if (res.succeeded()) {
                    ProtonConnection connection = res.result();

                    connection.setContainer(id);
                    connection.closeHandler(x -> {
                            completionLatch.countDown();
                        });

                    if (seconds > 0) {
                        vertx.setTimer(seconds * 1000, timerId -> {
                                stopping.lazySet(true);
                            });
                    }

                    if (sender) {
                        send(connection, path, messages, bodySize, durable, stopping);
                    } else {
                        receive(connection, path, messages, creditWindow, stopping);
                    }
                } else {
                    res.cause().printStackTrace();
                    completionLatch.countDown();
                }
            });

        // Await the operations completing, then shut down the Vertx
        // instance.
        completionLatch.await();

        vertx.close();
    }

    // TODO: adjust? The writer is [needlessly] synchronizing every
    // write, the buffer may flush more/less often than desired?.
    private static PrintWriter getOutputWriter() {
        return new PrintWriter(System.out);
    }

    private static void send(ProtonConnection connection, String address,
                             int messages, int bodySize, boolean durable,
                             AtomicBoolean stopping) {
        connection.open();

        final StringBuilder line = new StringBuilder();
        final long startTime = System.currentTimeMillis();
        final PrintWriter out = getOutputWriter();
        final AtomicLong count = new AtomicLong(1);
        final ProtonSender sender = connection.createSender(address);
        final byte[] body = new byte[bodySize];

        Arrays.fill(body, (byte) 120);

        sender.sendQueueDrainHandler(s -> {
                while (!sender.sendQueueFull()) {
                    Message msg = Message.Factory.create();

                    String id = String.valueOf(count.get());
                    msg.setMessageId(id);

                    msg.setBody(new Data(new Binary(body)));

                    Map<String, Object> props = new HashMap<>();
                    msg.setApplicationProperties(new ApplicationProperties(props));
                    long stime = System.currentTimeMillis();
                    props.put("SendTime", stime);

                    if (durable) {
                        msg.setDurable(true);
                    }

                    sender.send(msg);

                    line.setLength(0);
                    out.append(line.append(id).append(',').append(stime).append('\n'));

                    long cnt = count.getAndIncrement();

                    if (cnt >= messages || stopping.get()) {
                        out.flush();
                        connection.close();
                    }
                }
            });
        sender.open();
    }

    private static void receive(ProtonConnection connection, String address,
                                int messages, int creditWindow,
                                AtomicBoolean stopping) {
        connection.open();

        final StringBuilder line = new StringBuilder();
        final long startTime = System.currentTimeMillis();
        final PrintWriter out = getOutputWriter();
        final AtomicInteger count = new AtomicInteger(1);
        final ProtonReceiver receiver = connection.createReceiver(address);

        int creditTopUpThreshold = Math.max(1, creditWindow / 2);

        receiver.setAutoAccept(false).setPrefetch(0).flow(creditWindow);
        receiver.handler((delivery, msg) -> {
                Object id = msg.getMessageId();
                long stime = (Long) msg.getApplicationProperties().getValue().get("SendTime");
                long rtime = System.currentTimeMillis();

                line.setLength(0);
                out.append(line.append(id).append(',').append(stime).append(',').append(rtime).append('\n'));

                delivery.disposition(ACCEPTED, true);

                int credit = receiver.getCredit();
                if(credit < creditTopUpThreshold) {
                    receiver.flow(creditWindow - credit);
                }

                if (count.getAndIncrement() >= messages || stopping.get()) {
                    out.flush();
                    connection.close();
                }
            }).open();
    }
}
