/*
 * Copyright 2011-2022 the original author or authors.
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
package io.lettuce.core.protocol;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisException;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.internal.LettuceSets;
import io.lettuce.core.resource.ClientResources;
import io.netty.channel.*;
import io.netty.util.internal.logging.InternalLogLevel;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A netty {@link ChannelHandler} responsible for writing redis commands and reading responses from the server.
 *
 * @author Will Glozer
 * @author Mark Paluch
 * @author Jongyeol Choi
 * @author Grzegorz Szpak
 * @author Daniel Albuquerque
 * @author Gavin Cook
 * @author Anuraag Agrawal
 */
public class FlushHandler extends ChannelOutboundHandlerAdapter implements HasQueuedCommands {

    /**
     * When we encounter an unexpected IOException we look for these {@link Throwable#getMessage() messages} (because we have no
     * better way to distinguish) and log them at DEBUG rather than WARN, since they are generally caused by unclean client
     * disconnects rather than an actual problem.
     */
    static final Set<String> SUPPRESS_IO_EXCEPTION_MESSAGES = LettuceSets.unmodifiableSet("Connection reset by peer",
        "Broken pipe", "Connection timed out");

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(FlushHandler.class);

    private static final AtomicLong FLUSH_HANDLER_COUNTER = new AtomicLong();

    private final Reliability reliability;
    private final ClientOptions clientOptions;

    private final ClientResources clientResources;

    private final Endpoint endpoint;

    private final ArrayDeque<RedisCommand<?, ?, ?>> commandQueue = new ArrayDeque<>();

    private boolean autoFlushCommands = true;

    private boolean autoBatchCommands = false;

    private Duration autoBatchDelay = Duration.ofMillis(5);

    private int autoBatchSize = 500;

    private ScheduledFuture<?> autoBatchFuture;

    private final long flushHandlerId = FLUSH_HANDLER_COUNTER.incrementAndGet();

    private final boolean debugEnabled = logger.isDebugEnabled();

    private ChannelHandlerContext channelCtx;

//    private LifecycleState lifecycleState = LifecycleState.NOT_CONNECTED;

    private String logPrefix;

    /**
     * Initialize a new instance that handles commands from the supplied queue.
     *
     * @param clientOptions client options for this connection, must not be {@code null}
     * @param clientResources client resources for this connection, must not be {@code null}
     * @param endpoint must not be {@code null}.
     */
    public FlushHandler(ClientOptions clientOptions, ClientResources clientResources, Endpoint endpoint) {

        LettuceAssert.notNull(clientOptions, "ClientOptions must not be null");
        LettuceAssert.notNull(clientResources, "ClientResources must not be null");
        LettuceAssert.notNull(endpoint, "RedisEndpoint must not be null");

        this.clientOptions = clientOptions;
        this.clientResources = clientResources;
        this.endpoint = endpoint;
        this.reliability = clientOptions.isAutoReconnect() ? Reliability.AT_LEAST_ONCE : Reliability.AT_MOST_ONCE;
    }

    public Endpoint getEndpoint() {
        return endpoint;
    }

    public Queue<RedisCommand<?, ?, ?>> getCommandQueue() {
        return commandQueue;
    }

//    protected void setState(LifecycleState lifecycleState) {
//
//        if (this.lifecycleState != LifecycleState.CLOSED) {
//            this.lifecycleState = lifecycleState;
//        }
//    }

    @Override
    public Collection<RedisCommand<?, ?, ?>> drainQueue() {
        return drainCommands(commandQueue);
    }

//    protected LifecycleState getState() {
//        return lifecycleState;
//    }

//    public boolean isClosed() {
//        return lifecycleState == LifecycleState.CLOSED;
//    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {

        InternalLogLevel logLevel = InternalLogLevel.WARN;

        if (!commandQueue.isEmpty()) {
            RedisCommand<?, ?, ?> command = commandQueue.poll();
            if (debugEnabled) {
                logger.debug("{} Storing exception in {}", logPrefix(), command);
            }
            logLevel = InternalLogLevel.DEBUG;

            try {
                command.completeExceptionally(cause);
            } catch (Exception ex) {
                logger.warn("{} Unexpected exception during command completion exceptionally: {}", logPrefix, ex.toString(),
                        ex);
            }
        }

//        if (channel == null || !channel.channel().isActive() || !isConnected()) {
        if (channelCtx == null || !channelCtx.channel().isActive()) {

            if (debugEnabled) {
                logger.debug("{} Storing exception in connectionError", logPrefix());
            }

            logLevel = InternalLogLevel.DEBUG;
            endpoint.notifyException(cause);
        }

        if (cause instanceof IOException && logLevel.ordinal() > InternalLogLevel.INFO.ordinal()) {
            logLevel = InternalLogLevel.INFO;
            if (SUPPRESS_IO_EXCEPTION_MESSAGES.contains(cause.getMessage())) {
                logLevel = InternalLogLevel.DEBUG;
            }
        }

        logger.log(logLevel, "{} Unexpected exception during request: {}", logPrefix, cause.toString(), cause);
    }

    private static <T> List<T> drainCommands(Queue<T> source) {

        List<T> target = new ArrayList<>(source.size());

        T cmd;
        while ((cmd = source.poll()) != null) {
            target.add(cmd);
        }

        return target;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        this.channelCtx = ctx;
    }

    public void setAutoFlushCommands(boolean autoFlush) {
        this.autoFlushCommands = autoFlush;
        setAutoBatchCommands(false);
    }

    public void setAutoBatchCommands(boolean autoBatch) {
        this.autoBatchCommands = autoBatch;
        if (autoBatchCommands) {
            this.autoFlushCommands = false;
            startAutoBatch();
        } else {
            stopAutoBatch();
        }
    }

    public void setAutoBatchDelay(Duration delay) {
        this.autoBatchDelay = delay;
        if (autoBatchCommands) {
            startAutoBatch();
        }
    }

    public void setAutoBatchSize(int size) {
        this.autoBatchSize = size;
    }


    private void startAutoBatch() {
        long nanos = autoBatchDelay.toNanos(); // TODO confirm if sub-ms is worth it
        if (autoBatchFuture != null) {
            autoBatchFuture.cancel(false);
        }
        autoBatchFuture = channelCtx.channel().eventLoop().scheduleWithFixedDelay(this::autoFlush, nanos, nanos, TimeUnit.NANOSECONDS);
    }

    private void stopAutoBatch() {
        if (autoBatchFuture != null) {
            autoBatchFuture.cancel(false);
        }
    }

    private void autoFlush() {
        if (commandQueue.size() > 0) {
            flushCommands(commandQueue);
        }
    }

    /**
     * @see ChannelDuplexHandler#write(ChannelHandlerContext, Object,
     *      ChannelPromise)
     */
    @Override
    @SuppressWarnings("unchecked")
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {

        if (debugEnabled) {
            logger.debug("{} write(ctx, {}, promise)", logPrefix(), msg);
        }

        if (msg instanceof RedisCommand) {
            writeSingleCommand(ctx, (RedisCommand<?, ?, ?>) msg);
            return;
        }

        if (msg instanceof List) {

            List<RedisCommand<?, ?, ?>> batch = (List<RedisCommand<?, ?, ?>>) msg;

            if (batch.size() == 1) {

                writeSingleCommand(ctx, batch.get(0));
                return;
            }

            writeBatch(ctx, batch);
            return;
        }

        if (msg instanceof Collection) {
            writeBatch(ctx, (Collection<RedisCommand<?, ?, ?>>) msg);
        }
    }

    private void writeSingleCommand(ChannelHandlerContext ctx, RedisCommand<?, ?, ?> command) {

        if (!isWriteable(command)) {
            return;
        }
        if (autoFlushCommands) {
            ctx.write(command);
        } else {
            commandQueue.add(command);
            if (autoBatchCommands && commandQueue.size() >= autoBatchSize) {
                flushCommands();
            }
        }
    }

    private void writeBatch(ChannelHandlerContext ctx, Collection<RedisCommand<?, ?, ?>> batch) {

        Collection<RedisCommand<?, ?, ?>> deduplicated = new LinkedHashSet<>(batch.size(), 1);

        for (RedisCommand<?, ?, ?> command : batch) {

            if (isWriteable(command) && !deduplicated.add(command)) {
                deduplicated.remove(command);
                command.completeExceptionally(
                        new RedisException("Attempting to write duplicate command that is already enqueued: " + command));
            }
        }

        if (deduplicated.isEmpty()) {
            return;
        }

        if (autoFlushCommands) {
            ctx.write(deduplicated);
        } else {
            commandQueue.addAll(deduplicated);
            if (autoBatchCommands && commandQueue.size() >= autoBatchSize) {
                flushCommands();
            }
        }
    }

    public void flushCommands() {
        flushCommands(drainQueue());
    }

    private void flushCommands(Collection<RedisCommand<?, ?, ?>> commands) {
        if (debugEnabled) {
            logger.debug("{} flushCommands()", logPrefix());
        }

//        if (isConnected()) {
            if (commands.isEmpty()) {
                return;
            }

            if (debugEnabled) {
                logger.debug("{} flushCommands() Flushing {} commands", logPrefix(), commands.size());
            }

            if (!commands.isEmpty()) {
                writeAndFlush(commands);
            }
//        }
    }

    private void writeAndFlush(Collection<? extends RedisCommand<?, ?, ?>> commands) {

        if (reliability == Reliability.AT_MOST_ONCE) {

            // cancel on exceptions and remove from queue, because there is no housekeeping
            for (RedisCommand<?, ?, ?> command : commands) {
                channelCtx.pipeline().write(command);
                // TODO
            }
        }

        if (reliability == Reliability.AT_LEAST_ONCE) {

            // commands are ok to stay within the queue, reconnect will retrigger them
            for (RedisCommand<?, ?, ?> command : commands) {
                channelCtx.write(command);
                // TODO
            }
        }
        channelCtx.flush();
    }

    private static boolean isWriteable(RedisCommand<?, ?, ?> command) {
        return !command.isDone();
    }

    /**
     * Decoding hook: Complete a command.
     *
     * @param command
     * @see RedisCommand#complete()
     */
    protected void complete(RedisCommand<?, ?, ?> command) {
        command.complete();
    }

//    boolean isConnected() {
//        return lifecycleState.ordinal() >= LifecycleState.CONNECTED.ordinal()
//                && lifecycleState.ordinal() < LifecycleState.DISCONNECTED.ordinal();
//    }

    /**
     * @return the channel Id.
     * @since 6.1
     */
    public String getChannelId() {
        return channelCtx == null ? "unknown" : ChannelLogDescriptor.getId(channelCtx.channel());
    }

    private String logPrefix() {

        if (logPrefix != null) {
            return logPrefix;
        }

        String buffer = "[" + ChannelLogDescriptor.logDescriptor(channelCtx.channel()) + ", epid=" + endpoint.getId() + ", chid=0x"
                + getFlushHandlerId() + ']';
        return logPrefix = buffer;
    }

    private String getFlushHandlerId() {
        return Long.toHexString(flushHandlerId);
    }

    private static long nanoTime() {
        return System.nanoTime();
    }

    public enum LifecycleState {
        NOT_CONNECTED, REGISTERED, CONNECTED, ACTIVATING, ACTIVE, DISCONNECTED, DEACTIVATING, DEACTIVATED, CLOSED,
    }

    private enum Reliability {
        AT_MOST_ONCE, AT_LEAST_ONCE
    }
}
