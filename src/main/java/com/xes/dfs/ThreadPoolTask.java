package com.xes.dfs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.Channels;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

/**
 * Created by liaozhisong on 11/27/14.
 */
public class ThreadPoolTask implements Runnable, Serializable {

    public ThreadPoolTask(AsynchronousSocketChannel socketChannel) {
        this.socketChannel = socketChannel;
    }

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private AsynchronousSocketChannel socketChannel;

    public static final String SAVE_PATH = "/Users/liaozhisong/Documents/temp/";

    public static final int BUFFER_SIZE = 1024000;

    public static final SimpleDateFormat sdf = new SimpleDateFormat("YYYYMMddHHmmss");

    @Override
    public void run() {
        DataInputStream dis = null;
        InputStream connectionInputStream = null;
        DataOutputStream dos = null;
        try {
            connectionInputStream = Channels.newInputStream(socketChannel);
            dis = new DataInputStream(connectionInputStream);
            // 本地保存路径，文件名会自动从服务器端继承而来。
            byte[] buf = new byte[BUFFER_SIZE];
            int passedLen = 0;
            String filePath = SAVE_PATH + getRandomString(8) + "_" + dis.readUTF();
            long len = dis.readLong();
            dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(filePath)));
            logger.info("开始接收文件! File Size()：" + len + "bytes");
            while (passedLen < len) {
                int read = dis.read(buf);
                passedLen += read;
                logger.info("文件接收了" + (passedLen * 100 / len) + "%\n");
                dos.write(buf, 0, read);
            }
            String replyMsg = "文件路径：" + filePath;
            logger.info(replyMsg + "\n");
            socketChannel.write(ByteBuffer.wrap(replyMsg.getBytes()));
        } catch (IOException e) {
            logger.error("", e);
        } finally {
            try {
                if (dos != null)
                    dos.close();
                if (connectionInputStream != null)
                    connectionInputStream.close();
                if (dis != null)
                    dis.close();
                if (socketChannel.isOpen())
                    socketChannel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static String getRandomString(int length) { //length表示生成字符串的长度
        String base = "abcdefghijklmnopqrstuvwxyz0123456789";
        Random random = new Random();
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < length; i++) {
            int number = random.nextInt(base.length());
            sb.append(base.charAt(number));
        }
        return sb.toString();
    }

}
