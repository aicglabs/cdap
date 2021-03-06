/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.runtime.spi.runtimejob;

/**
 * Uniquely identifies a runtime job.
 */
public class RuntimeJobId {
  private final String runtimeJobId;

  public RuntimeJobId(String runtimeJobId) {
    this.runtimeJobId = runtimeJobId;
  }

  /**
   * Returns a runtime job id.
   */
  public String getRuntimeJobId() {
    return runtimeJobId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RuntimeJobId that = (RuntimeJobId) o;
    return runtimeJobId.equals(that.runtimeJobId);
  }

  @Override
  public int hashCode() {
    return runtimeJobId.hashCode();
  }
}
