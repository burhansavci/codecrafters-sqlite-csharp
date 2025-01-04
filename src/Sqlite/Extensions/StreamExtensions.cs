using System.Buffers.Binary;
using System.Text;

namespace codecrafters_sqlite.Sqlite.Extensions;

public static class StreamExtensions
{
    // Each byte contributes 7 bits of data
    private const byte VarintEncodingBits = 7;

    // The upper bit indicates whether there are more bytes
    private const byte VarintContinueFlag = 1 << VarintEncodingBits;

    public static int ReadVarint(this Stream stream)
    {
        var value = 0;
        var length = 0; // the number of bits of data read so far

        while (true)
        {
            (var byteValue, var moreBytes) = ReadVarintByte(stream);

            // Add in the data bits
            value |= byteValue << length;

            // Stop if this is the last byte
            if (!moreBytes)
                return value;

            length += VarintEncodingBits;
        }
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
        var value = (byte)(byteValue & ~VarintContinueFlag);
        var moreBytes = (byteValue & VarintContinueFlag) != 0;

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