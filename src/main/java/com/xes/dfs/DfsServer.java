package com.xes.dfs;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import static java.nio.channels.AsynchronousChannelGroup.withCachedThreadPool;

/**
 * Created by liaozhisong on 11/12/14.
 */
public class DfsServer {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    public static final int PORT = 9888;

    public static final int POOL_SIZE = 10;

    public void startWithCompletionHandler() throws InterruptedException,
            ExecutionException, TimeoutException, IOException {
        logger.info("Server listen on " + PORT);
        AsynchronousChannelGroup channelGroup = withCachedThreadPool(Executors.newCachedThreadPool(), POOL_SIZE);
        final AsynchronousServerSocketChannel server = AsynchronousServerSocketChannel
                .open(channelGroup)
                .setOption(StandardSocketOptions.SO_REUSEADDR, true)
                .setOption(StandardSocketOptions.SO_RCVBUF, 16 * 1024)
                .bind(new InetSocketAddress(PORT), 100);
        final ExecutorService executorService = Executors.newFixedThreadPool(10);
        //注册事件和事件完成后的处理器
        server.accept(null,
                new CompletionHandler<AsynchronousSocketChannel, Object>() {
                    public void completed(final AsynchronousSocketChannel socketChannel,
                                          Object attachment) {
                        try {
                            logger.info("Accept connection from " + socketChannel.getRemoteAddress());
                            ThreadPoolTask task = new ThreadPoolTask(socketChannel);
                            executorService.execute(task);
                        } catch (Exception e) {
                            try {
                                //异常情况需关闭连接
                                if (socketChannel.isOpen())
                                    socketChannel.close();
                            } catch (IOException e1) {
                                e1.printStackTrace();
                            }
                            logger.error("", e);
                        } finally {
                            server.accept(null, this);
                        }
                        logger.info("end");
                    }
                    @Override
                    public void failed(Throwable exc, Object attachment) {
                        logger.info("", exc);
                    }
                });
        // 主线程继续自己的行为
        while (true) {
            logger.info("main thread");
            Thread.sleep(10000);
        }
    }

    public static void main(String args[]) throws Exception {
        new DfsServer().startWithCompletionHandler();
    }
}
