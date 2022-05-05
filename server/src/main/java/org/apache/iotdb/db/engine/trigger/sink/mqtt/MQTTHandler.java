/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.engine.trigger.sink.mqtt;

import java.util.List;
import org.apache.iotdb.db.engine.trigger.sink.api.Handler;
import org.apache.iotdb.db.engine.trigger.sink.exception.SinkException;
import org.apache.iotdb.tsfile.utils.Binary;

import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;

import java.net.InetAddress;

public class MQTTHandler implements Handler<MQTTConfiguration, MQTTEvent> {

  private BlockingConnection connection;

  private String payloadFormatter;

  @Override
  public void open(MQTTConfiguration configuration) throws Exception {
    MQTT mqtt = new MQTT();
    ping(configuration.getHost());
    mqtt.setHost(configuration.getHost(), configuration.getPort());
    mqtt.setUserName(configuration.getUsername());
    mqtt.setPassword(configuration.getPassword());
    mqtt.setConnectAttemptsMax(configuration.getConnectAttemptsMax());
    mqtt.setReconnectDelay(configuration.getReconnectDelay());
    connection = mqtt.blockingConnection();
    connection.connect();
    payloadFormatter = generatePayloadFormatter(configuration);
  }

  private void ping(String host) throws Exception {
    InetAddress inet = InetAddress.getByName(host);
    if (!inet.isReachable(1000)) {
      throw new Exception("Connection refused");
    }
  }

  private static String generatePayloadFormatter(MQTTConfiguration configuration)
      throws SinkException {
    return String.format(
        "{\"device\":\"%s\",\"measurements\":[%s]%s",
        configuration.getDevice(),
        arrayToJson(configuration.getMeasurements()),
        ",\"timestamp\":%d,\"values\":[%s]}");
  }

  private static String arrayToJson(Object[] array) throws SinkException {
    if (array.length <= 0) {
      throw new SinkException("The number of measurements should be positive.");
    }

    StringBuilder sb = new StringBuilder(objectToJson(array[0]));
    for (int i = 1; i < array.length; ++i) {
      sb.append(',').append(objectToJson(array[i]));
    }
    return sb.toString();
  }

  private static String objectToJson(Object object) {
    return (object instanceof String || object instanceof Binary)
        ? ('\"' + object.toString() + '\"')
        : object.toString();
  }

  @Override
  public void close() throws Exception {
    connection.disconnect();
  }

  @Override
  public void onEvent(MQTTEvent event) throws Exception {
    String payload =
        String.format(payloadFormatter, event.getTimestamp(), arrayToJson(event.getValues()));
    connection.publish(event.getTopic(), payload.getBytes(), event.getQoS(), event.retain());
  }
  @Override
  public void onEvent(List<MQTTEvent> events) throws Exception {
//    String payload =
//        String.format(payloadFormatter, event.getTimestamp(), arrayToJson(event.getValues()));
//    connection.publish(event.getTopic(), payload.getBytes(), event.getQoS(), event.retain());
  }
}
