package com.xes.dfs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;


/**
 * Created by liaozhisong on 11/26/14.
 */
public class DfsClient {

    private final static Logger logger = LoggerFactory.getLogger(DfsClient.class);

    private final static String IP = "localhost";

    private final static int PORT = 9888;

    public static final int BUFFER_SIZE = 1024000;

    public static void main(String[] args) throws Exception {
        final String filePath = "/Users/liaozhisong/Downloads/MongoDB.pdf";
        for (int i = 0; i < 3; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    FileInputStream inputStream = null;
                    try {
                        File fi = new File(filePath);
                        inputStream = new FileInputStream(fi);
                        new DfsClient().upload(inputStream, fi.getName(), fi.length());
                    } catch (Exception e) {
                        logger.error("", e);
                    } finally {
                        if (inputStream != null)
                            try {
                                inputStream.close();
                            } catch (IOException e) {
                                logger.error("", e);
                            }
                    }
                }
            }).start();
        }
    }

    public void upload(InputStream in, String fileName, long fileLength) {
        AsynchronousSocketChannel client = null;
        DataInputStream fis = null;
        OutputStream os = null;
        DataOutputStream dos = null;
        try {
            client = AsynchronousSocketChannel.open();
            client.connect(new InetSocketAddress(IP, PORT)).get();
            os = Channels.newOutputStream(client);
            logger.info("File Name：" + fileName + ";\tFile Size()：" + fileLength + "bytes");
            // public Socket accept() throws
            // IOException侦听并接受到此套接字的连接。此方法在进行连接之前一直阻塞。
            fis = new DataInputStream(new BufferedInputStream(in));
            dos = new DataOutputStream(os);
            // 将文件名及长度传给客户端
            dos.writeUTF(fileName);
            dos.writeLong(fileLength);
            byte[] buf = new byte[BUFFER_SIZE];
            while (true) {
                int read = fis.read(buf);
                if (read == -1) {
                    break;
                }
                dos.write(buf, 0, read);
            }
            dos.flush();
            fis.close();
            logger.info("文件传输完成\n");
            ByteBuffer bb = ByteBuffer.allocate(10);
            List<byte[]> list = new ArrayList<>();
            int length = 0;
            for(int a = client.read(bb).get(); a != -1;a = client.read(bb).get()){
                bb.flip();
                byte[] b = new byte[bb.limit()];
                length += bb.limit();
                bb.get(b);
                list.add(b);
                bb.clear();
            }
            byte[] bRes = new byte[length];
            int pos = 0;
            for (byte[] b : list) {
                System.arraycopy(b, 0, bRes, pos, b.length);
                pos += b.length;
            }
            logger.info(new String(bRes));
        } catch (InterruptedException | ExecutionException | IOException e) {
            logger.error("", e);
        } finally {
            try {
                if (dos != null)
                    dos.close();
            } catch (IOException e) {
                logger.error("", e);
            }
            try {
                if (os != null)
                    os.close();
            } catch (IOException e) {
                logger.error("", e);
            }
            try {
                if (fis != null)
                    fis.close();
            } catch (IOException e) {
                logger.error("", e);
            }
            try {
                if (client != null)
                    client.close();
            } catch (IOException e) {
                logger.error("", e);
            }
        }

    }
}
