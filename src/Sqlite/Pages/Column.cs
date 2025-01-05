using System.Buffers.Binary;
using System.Text;

namespace codecrafters_sqlite.Sqlite.Pages;

public record Column
{
    public Column(SerialTypeCode serialTypeCode, int index, Stream recordStream)
    {
        SerialTypeCode = serialTypeCode;
        Index = index;
        
        var contentReadSize = serialTypeCode.ContentSize > recordStream.Length - recordStream.Position
            ? (int)(recordStream.Length - recordStream.Position)
            : serialTypeCode.ContentSize;

        var buffer = new byte[contentReadSize];
        recordStream.ReadExactly(buffer, 0, (int)contentReadSize);
        Value = GetValue(buffer);
    }

    public SerialTypeCode SerialTypeCode { get; }
    public object? Value { get; private set; }
    public int Index { get; set; }

    private object? GetValue(byte[] buffer)
    {
        return SerialTypeCode.Type switch
        {
            SerialType.Null => null,
            SerialType.Int8 => buffer[0],
            SerialType.Int16 => BinaryPrimitives.ReadInt16BigEndian(buffer),
            SerialType.Int24 => buffer[0] | buffer[1] << 8 | buffer[2] << 16,
            SerialType.Int32 => BinaryPrimitives.ReadInt32BigEndian(buffer),
            SerialType.Int48 => buffer[0] | buffer[1] << 8 | buffer[2] << 16 | buffer[3] << 24 | buffer[4] << 32 | buffer[5] << 40,
            SerialType.Int64 => BinaryPrimitives.ReadInt64BigEndian(buffer),
            SerialType.Float64 => BinaryPrimitives.ReadDoubleBigEndian(buffer),
            SerialType.IntegerZero => 0,
            SerialType.IntegerOne => 1,
            SerialType.Reserved => null,
            SerialType.Reserved2 => null,
            SerialType.Blob => buffer,
            SerialType.String => Encoding.UTF8.GetString(buffer),
            _ => throw new ArgumentOutOfRangeException(nameof(SerialTypeCode.Type), SerialTypeCode.Type, "Invalid serial type")
        };
    }
}