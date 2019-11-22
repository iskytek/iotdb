/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.server.handlers.caller;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient.sendSnapshot_call;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnapshotCatchUpHandler implements AsyncMethodCallback<sendSnapshot_call> {

  private static final Logger logger = LoggerFactory.getLogger(SnapshotCatchUpHandler.class);

  private AtomicBoolean succeed;
  private Node receiver;
  private Snapshot snapshot;

  public SnapshotCatchUpHandler(AtomicBoolean succeed,
      Node receiver, Snapshot snapshot) {
    this.succeed = succeed;
    this.receiver = receiver;
    this.snapshot = snapshot;
  }

  @Override
  public void onComplete(sendSnapshot_call response) {
    try {
      response.getResult();
      synchronized (succeed) {
        succeed.set(true);
        succeed.notifyAll();
      }
    } catch (TException e) {
      onError(e);
    }
  }

  @Override
  public void onError(Exception exception) {
    logger.error("Cannot send snapshot {} to {}", snapshot, receiver);
  }
}
