/*
 * Copyright 2014 Red Hat, Inc.
 *
 * Red Hat licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package metroinsight.citadel.model;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;

/**
 * Converter for {@link metroinsight.citadel.model.Datapoint}.
 *
 * NOTE: This class has been automatically generated from the {@link metroinsight.citadel.model.Datapoint} original class using Vert.x codegen.
 */
public class DatapointConverter {

  public static void fromJson(JsonObject json, Datapoint obj) {
    if (json.getValue("lat") instanceof String) {
      obj.setLat((String)json.getValue("lat"));
    }
    if (json.getValue("lng") instanceof String) {
      obj.setLng((String)json.getValue("lng"));
    }
    if (json.getValue("srcid") instanceof String) {
      obj.setSrcid((String)json.getValue("srcid"));
    }
    if (json.getValue("unixTimeStamp") instanceof String) {
      obj.setUnixTimeStamp((String)json.getValue("unixTimeStamp"));
    }
    if (json.getValue("value") instanceof String) {
      obj.setValue((String)json.getValue("value"));
    }
  }

  public static void toJson(Datapoint obj, JsonObject json) {
    if (obj.getLat() != null) {
      json.put("lat", obj.getLat());
    }
    if (obj.getLng() != null) {
      json.put("lng", obj.getLng());
    }
    if (obj.getSrcid() != null) {
      json.put("srcid", obj.getSrcid());
    }
    if (obj.getUnixTimeStamp() != null) {
      json.put("unixTimeStamp", obj.getUnixTimeStamp());
    }
    if (obj.getValue() != null) {
      json.put("value", obj.getValue());
    }
  }
}