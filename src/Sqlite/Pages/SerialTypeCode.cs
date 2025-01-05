namespace codecrafters_sqlite.Sqlite.Pages;

// Serial Type Codes: https://www.sqlite.org/fileformat.html#record_format
public record SerialTypeCode
{
    public SerialTypeCode(long value)
    {
        switch (value)
        {
            case 0:
            {
                Value = value;
                Type = SerialType.Null;
                Name = "Null";
                ContentSize = 0;
                break;
            }
            case 1:
            {
                Value = value;
                Type = SerialType.Int8;
                Name = "Int8";
                ContentSize = 1;
                break;
            }
            case 2:
            {
                Value = value;
                Type = SerialType.Int16;
                Name = "Int16";
                ContentSize = 2;
                break;
            }
            case 3:
            {
                Value = value;
                Type = SerialType.Int24;
                Name = "Int24";
                ContentSize = 3;
                break;
            }
            case 4:
            {
                Value = value;
                Type = SerialType.Int32;
                Name = "Int32";
                ContentSize = 4;
                break;
            }
            case 5:
            {
                Value = value;
                Type = SerialType.Int48;
                Name = "Int48";
                ContentSize = 6;
                break;
            }
            case 6:
            {
                Value = value;
                Type = SerialType.Int64;
                Name = "Int64";
                ContentSize = 8;
                break;
            }
            case 7:
            {
                Value = value;
                Type = SerialType.Float64;
                Name = "Float64";
                ContentSize = 8;
                break;
            }
            case 8:
            {
                Value = value;
                Type = SerialType.IntegerZero;
                Name = "IntegerZero";
                ContentSize = 0;
                break;
            }
            case 9:
            {
                Value = value;
                Type = SerialType.IntegerOne;
                Name = "IntegerOne";
                ContentSize = 0;
                break;
            }
            case 10:
            {
                Value = value;
                Type = SerialType.Reserved;
                Name = "Reserved";
                ContentSize = -1;
                break;
            }
            case 11:
            {
                Value = value;
                Type = SerialType.Reserved2;
                Name = "Reserved2";
                ContentSize = -1;
                break;
            }
            case >= 12 when value % 2 == 0:
            {
                Value = value;
                Type = SerialType.Blob;
                Name = "Blob";
                ContentSize = (value - 12) / 2;
                break;
            }
            case >= 13 when value % 2 == 1:
            {
                Value = value;
                Type = SerialType.String;
                Name = "String";
                ContentSize = (value - 13) / 2;
                break;
            }
            default:
            {
                throw new ArgumentOutOfRangeException(nameof(value), value, "Invalid serial type code");
            }
        }
    }

    public long Value { get; init; }
    public SerialType Type { get; set; }
    public string Name { get; init; }
    public long ContentSize { get; init; }
}

public enum SerialType
{
    Null = 0,
    Int8 = 1,
    Int16 = 2,
    Int24 = 3,
    Int32 = 4,
    Int48 = 5,
    Int64 = 6,
    Float64 = 7,
    IntegerZero = 8,
    IntegerOne = 9,
    Reserved = 10,
    Reserved2 = 11,
    Blob = 12,
    String = 13
}