/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.model.debezium;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieDebeziumAvroPayloadException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

/**
 * Provides support for seamlessly applying changes captured via Debezium for SQLServer DB.
 * <p>
 * Debezium change event types are determined for the op field in the payload
 * <p>
 * - For inserts, op=i
 * - For deletes, op=d
 * - For updates, op=u
 * - For snapshot inserts, op=r
 * <p>
 * This payload implementation will issue matching insert, delete, updates against the hudi table
 */
public class SqlserverDebeziumAvroPayload extends AbstractDebeziumAvroPayload {

  private static final Logger LOG = LoggerFactory.getLogger(SqlserverDebeziumAvroPayload.class);

  public SqlserverDebeziumAvroPayload(GenericRecord record, Comparable orderingVal) {
    super(record, orderingVal);
  }

  public SqlserverDebeziumAvroPayload(Option<GenericRecord> record) {
    super(record);
  }

  private Option<String> extractLSN(IndexedRecord record) {
    Object value = ((GenericRecord) record).get(DebeziumConstants.FLATTENED_CHANGE_LSN_COL_NAME);
    return Option.ofNullable(Objects.toString(value, null));
  }

  @Override
  protected boolean shouldPickCurrentRecord(IndexedRecord currentRecord, IndexedRecord insertRecord, Schema schema) throws IOException {
    String insertSourceChangeLSN = extractLSN(insertRecord)
        .orElseThrow(() ->
            new HoodieDebeziumAvroPayloadException(String.format("%s cannot be null in insert record: %s",
                DebeziumConstants.FLATTENED_CHANGE_LSN_COL_NAME, insertRecord)));
    Option<String> currentSourceChangeLSN = extractLSN(currentRecord);

    return currentSourceChangeLSN.isPresent() && insertSourceChangeLSN.compareTo(currentSourceChangeLSN.get()) < 0;
  }
}
