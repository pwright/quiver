/*
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
 */
package net.ssorj.quiver;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import javax.jms.*;
import javax.naming.*;

public class QuiverArrowJms {
    public static void main(String[] args) {
        try {
            doMain(args);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static void doMain(String[] args) throws Exception {
        String connectionMode = args[0];
        String channelMode = args[1];
        String operation = args[2];
        String path = args[6];
        int seconds = Integer.parseInt(args[7]);
        int messages = Integer.parseInt(args[8]);
        int bodySize = Integer.parseInt(args[9]);
        int transactionSize = Integer.parseInt(args[11]);
        String[] flags = args[12].split(",");

        if (!connectionMode.equals("client")) {
            throw new RuntimeException("This impl supports client mode only");
        }

        if (!channelMode.equals("active")) {
            throw new RuntimeException("This impl supports active mode only");
        }

        String url = System.getProperty("arrow.jms.url");
        assert url != null;

        Hashtable<Object, Object> env = new Hashtable<Object, Object>();
        env.put("connectionFactory.ConnectionFactory", url);
        env.put("brokerURL", url);
        env.put("queue.queueLookup", path);

        Context context = new InitialContext(env);;
        ConnectionFactory factory = (ConnectionFactory) context.lookup("ConnectionFactory");
        Destination queue = (Destination) context.lookup("queueLookup");

        Client client = new Client(factory, queue, operation, seconds, messages, bodySize, transactionSize, flags);

        client.run();
    }
}

class Client {
    protected final ConnectionFactory factory;
    protected final Destination queue;
    protected final String operation;
    protected final int seconds;
    protected final int messages;
    protected final int bodySize;
    protected final int transactionSize;

    protected final boolean durable;

    protected long startTime;
    protected int sent;
    protected int received;
    protected AtomicBoolean stopping = new AtomicBoolean();

    Client(ConnectionFactory factory, Destination queue, String operation,
           int seconds, int messages, int bodySize, int transactionSize, String[] flags) {
        this.factory = factory;
        this.queue = queue;
        this.operation = operation;
        this.seconds = seconds;
        this.messages = messages;
        this.bodySize = bodySize;
        this.transactionSize = transactionSize;

        this.durable = Arrays.asList(flags).contains("durable");
    }

    void run() {
        try {
            Connection conn = factory.createConnection();
            conn.start();

            startTime = System.currentTimeMillis();

            if (seconds > 0) {
                Timer timer = new Timer(true);
                TimerTask task = new TimerTask() {
                        public void run() {
                            stopping.lazySet(true);
                        }
                    };

                timer.schedule(task, seconds * 1000);
            }

            final Session session;

            if (transactionSize > 0) {
                session = conn.createSession(true, Session.SESSION_TRANSACTED);
            } else {
                session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            }

            try {
                if (operation.equals("send")) {
                    sendMessages(session);
                } else if (operation.equals("receive")) {
                    receiveMessages(session);
                } else {
                    throw new java.lang.IllegalStateException();
                }
            } catch (JMSException e) {
                // Ignore error from remote close
                return;
            }

            if (transactionSize > 0) {
                session.commit();
            }

            conn.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (JMSException e) {
            throw new RuntimeException(e);
        }
    }

    private static BufferedWriter getWriter() {
        return new BufferedWriter(new OutputStreamWriter(System.out));
    }

    void sendMessages(Session session) throws IOException, JMSException {
        final StringBuilder line = new StringBuilder();
        final BufferedWriter out = getWriter();
        final MessageProducer producer = session.createProducer(queue);

        if (durable) {
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        } else {
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        }

        producer.setDisableMessageTimestamp(true);

        byte[] body = new byte[bodySize];
        Arrays.fill(body, (byte) 120);

        while (sent < messages && !stopping.get()) {
            BytesMessage message = session.createBytesMessage();
            long stime = System.currentTimeMillis();

            message.writeBytes(body);
            message.setLongProperty("SendTime", stime);

            producer.send(message);
            line.setLength(0);
            out.append(line.append(message.getJMSMessageID()).append(',').append(stime).append('\n'));

            sent += 1;

            if (transactionSize > 0 && (sent % transactionSize) == 0) {
                session.commit();
            }
        }

        out.flush();
    }

    void receiveMessages(Session session) throws IOException, JMSException {
        final StringBuilder line = new StringBuilder();
        final BufferedWriter out = getWriter();
        final MessageConsumer consumer = session.createConsumer(queue);

        while (received < messages && !stopping.get()) {
            Message message = consumer.receive();

            if (message == null) {
                throw new RuntimeException("Null receive");
            }

            String id = message.getJMSMessageID();
            long stime = message.getLongProperty("SendTime");
            long rtime = System.currentTimeMillis();

            line.setLength(0);
            out.append(line.append(id).append(',').append(stime).append(',').append(rtime).append('\n'));

            received += 1;

            if (transactionSize > 0 && (received % transactionSize) == 0) {
                session.commit();
            }
        }

        out.flush();
    }
}
