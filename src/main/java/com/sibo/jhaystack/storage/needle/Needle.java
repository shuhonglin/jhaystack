package com.sibo.jhaystack.storage.needle;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

import java.io.DataOutput;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

/**
 * @author YUI
 * @description
 * @date 2018-8-10 10:19.
 */
public class Needle {

    public final static byte MAGIC_HEADER = 0X01;

    public final static int NEEDLE_PADDING_SIZE = 8;
    public final static int NEEDLE_ENTRY_SIZE = 16;
    public final static int TIMESTAMP_SIZE = 8;
    public final static int TTL_SIZE = 4;
    public final static int CRC_SIZE = 4;

    private ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer();
    private int cookie;
    private long needleId;
    private int size;

    private int dataSize;
    private byte[] data;
    private byte flag;
    private short nameSize;
    private byte[] name;

    private long lastModified;
    private int ttl;

    private int crc;
    private byte[] padding;


    public void append(FileChannel fileChannel) throws IOException {
        buf.clear();
        buf.writeByte(MAGIC_HEADER);
        buf.writeInt(this.cookie);
        buf.writeLong(this.needleId);
        buf.writeInt(this.size);
        buf.writeInt(this.dataSize);
        buf.writeBytes(this.data);
        buf.writeByte(this.flag);
        buf.writeShort(this.nameSize);
        buf.writeBytes(this.name);
        buf.writeLong(this.lastModified);
        buf.writeInt(this.ttl);
        buf.writeInt(this.crc);
        buf.writeZero(calPaddingSize());
        buf.readBytes(fileChannel, fileChannel.size(), buf.readableBytes());
    }
    
    public byte[] read(FileChannel fileChannel, int offset, int length) throws IOException {
        buf.clear();
        buf.writeBytes(fileChannel, offset, length);
        byte[] target = new byte[buf.readableBytes()];
        buf.readBytes(target);
        return target;
    }

    private int calPaddingSize() {
        return NEEDLE_PADDING_SIZE - (NEEDLE_ENTRY_SIZE+this.size+TIMESTAMP_SIZE+TTL_SIZE+CRC_SIZE)%NEEDLE_PADDING_SIZE;
    }

}
