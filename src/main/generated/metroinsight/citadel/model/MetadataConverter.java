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
 * Converter for {@link metroinsight.citadel.model.Metadata}.
 *
 * NOTE: This class has been automatically generated from the {@link metroinsight.citadel.model.Metadata} original class using Vert.x codegen.
 */
public class MetadataConverter {

  public static void fromJson(JsonObject json, Metadata obj) {
    if (json.getValue("pointType") instanceof String) {
      obj.setPointType((String)json.getValue("pointType"));
    }
    if (json.getValue("srcid") instanceof String) {
      obj.setSrcid((String)json.getValue("srcid"));
    }
    if (json.getValue("unit") instanceof String) {
      obj.setUnit((String)json.getValue("unit"));
    }
  }

  public static void toJson(Metadata obj, JsonObject json) {
    if (obj.getPointType() != null) {
      json.put("pointType", obj.getPointType());
    }
    if (obj.getSrcid() != null) {
      json.put("srcid", obj.getSrcid());
    }
    if (obj.getUnit() != null) {
      json.put("unit", obj.getUnit());
    }
  }
}