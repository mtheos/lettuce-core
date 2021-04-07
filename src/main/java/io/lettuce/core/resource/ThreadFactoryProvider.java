/*
 * Copyright 2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lettuce.core.resource;

import java.util.concurrent.ThreadFactory;

/**
 * Interface to provide a custom {@link java.util.concurrent.ThreadFactory}. Implementations are asked through
 * {@link #getThreadFactory(String)} to provide a thread factory for a given pool name.
 *
 * @since 6.1.1
 */
@FunctionalInterface
public interface ThreadFactoryProvider {

    /**
     * Return a {@link ThreadFactory} for the given {@code poolName}.
     *
     * @param poolName a descriptive pool name. Typically used as prefix for thread names.
     * @return the {@link ThreadFactory}.
     */
    ThreadFactory getThreadFactory(String poolName);

}
