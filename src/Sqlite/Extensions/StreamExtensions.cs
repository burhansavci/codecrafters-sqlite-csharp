using System.Buffers.Binary;
using System.Text;

namespace codecrafters_sqlite.Sqlite.Extensions;

public static class StreamExtensions
{
    // Each byte contributes 7 bits of data except for the last byte which contributes 8 bits
    private const byte VarintEncodingBits = 7;

    // The upper bit indicates whether there are more bytes
    private const byte VarintContinueFlag = 1 << VarintEncodingBits;

    public static long ReadVarint(this Stream stream)
    {
        long value = 0;
        int bytesRead = 0;

        while (bytesRead < 9)
        {
            var (byteValue, moreBytes) = ReadVarintByte(stream);
            bytesRead++;

            if (bytesRead == 9)
            {
                // On the 9th byte, we use all 8 bits and don't check the continuation bit
                value = (value << 8) | byteValue;
                return value;
            }

            // For bytes 1-8, we only use 7 bits
            value = (value << VarintEncodingBits) | byteValue;

            if (!moreBytes)
                return value;
        }

        throw new InvalidDataException("Varint is too large");
    }

    private static (byte Value, bool MoreBytes) ReadVarintByte(Stream stream)
    {
        var buffer = new byte[1];
        var bytesRead = stream.Read(buffer, 0, 1);

        if (bytesRead != 1)
        {
            throw new IOException("Failed to read the required byte from the stream.");
        }

        var byteValue = buffer[0];
        var moreBytes = (byteValue & VarintContinueFlag) != 0;
        var value = (byte)(byteValue & ~VarintContinueFlag);

        return (value, moreBytes);
    }

    public static byte[] ReadBytes(this Stream stream, int length)
    {
        var buffer = new byte[length];
        stream.ReadExactly(buffer, 0, length);
        return buffer;
    }

    public static string ReadString(this Stream stream, int length)
    {
        var buffer = new byte[length];
        stream.ReadExactly(buffer, 0, length);
        return Encoding.UTF8.GetString(buffer);
    }

    public static ushort ReadUInt16BigEndian(this Stream stream)
    {
        var buffer = new byte[2];
        stream.ReadExactly(buffer, 0, 2);
        return BinaryPrimitives.ReadUInt16BigEndian(buffer);
    }

    public static uint ReadUInt32BigEndian(this Stream stream)
    {
        var buffer = new byte[4];
        stream.ReadExactly(buffer, 0, 4);
        return BinaryPrimitives.ReadUInt32BigEndian(buffer);
    }
}