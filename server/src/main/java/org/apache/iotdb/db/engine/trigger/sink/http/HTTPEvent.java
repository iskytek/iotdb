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

package org.apache.iotdb.db.engine.trigger.sink.http;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iotdb.db.engine.trigger.sink.api.Event;
import org.apache.iotdb.db.engine.trigger.sink.exception.SinkException;

public class HTTPEvent implements Event {

  private static final String PARAMETER_NULL_ERROR_STR = "parameter null error";

  // json 格式转发 还是 自定义格式
  private static final String formatter = "";

  private static final Pattern pattern = Pattern.compile("\\{\\{\\.\\w+}}");

  private String fullPath = null;

  public HTTPEvent(String fullPath) throws SinkException {
    this.fullPath = fullPath;
  }

  public String toJsonString() {
    Gson gson = new Gson();
    Type gsonType = new TypeToken<Map>() {
    }.getType();
    StringBuilder sb = new StringBuilder();

    return sb.toString();
  }

  private static String fillTemplate(Map<String, String> map, String template) {
    if (template == null || map == null) {
      return null;
    }
    StringBuffer sb = new StringBuffer();
    Matcher m = pattern.matcher(template);
    while (m.find()) {
      String param = m.group();
      String key = param.substring(3, param.length() - 2).trim();
      String value = map.get(key);
      m.appendReplacement(sb, value == null ? "" : value);
    }
    m.appendTail(sb);
    return sb.toString();
  }

  public String getFullPath() {
    return fullPath;
  }
}
