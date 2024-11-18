/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ratis.metrics.impl;

import com.codahale.metrics.jmx.ObjectNameFactory;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

class RatisObjectNameFactory implements ObjectNameFactory {
  @Override
  public ObjectName createName(String type, String domain, String name) {
    try {
      ObjectName objectName = new ObjectName(domain, "name", name);
      if (objectName.isPattern()) {
        objectName = new ObjectName(domain, "name", ObjectName.quote(name));
      }
      return objectName;
    } catch (MalformedObjectNameException e) {
      try {
        return new ObjectName(domain, "name", ObjectName.quote(name));
      } catch (MalformedObjectNameException mone) {
        throw new IllegalArgumentException(
            "Failed to register " + name + ", type=" + type + ", domain=" + domain, mone);
      }
    }
  }
}
