/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common;

import java.util.Map;

/**
 * A Mix-in(混合) style interface for classes that are instantiated by reflection and need to take configuration parameters
 *
 *设计Configurable接口的目的是统一反射后的初始化过程,对外提供统一的初始化接口
 * 在AbstractConfig.getConfiguredInstance()方法中通过反射构造出来的对象，都是通过无参构造函数构造的
 * 需要初始化的字段个类型各式各样 Configurable接口configure()方法封装了对象初始化过程且只有一个参数
 * 这样对外的接口就变得统一
 *
 */
public interface Configurable {

    /**
     * Configure this class with the given key-value pairs
     */
    public void configure(Map<String, ?> configs);

}
