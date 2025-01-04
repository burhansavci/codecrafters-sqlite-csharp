using codecrafters_sqlite.Sqlite.Extensions;

namespace codecrafters_sqlite.Sqlite.Pages;

// Record Format: https://www.sqlite.org/fileformat.html#record_format
public record Record
{
    public Record(Stream recordStream)
    {
        ArgumentNullException.ThrowIfNull(recordStream, nameof(recordStream));

        if (recordStream.CanRead == false)
            throw new ArgumentException("The stream must be readable", nameof(recordStream));

        // Read the header of the record
        HeaderSize = recordStream.ReadVarint();
        SerialTypeCodes = GetSerialTypeCodes(recordStream);

        // Read the body of the record
        Columns = GetColumns(recordStream);
    }

    public int HeaderSize { get; private init; }
    public SerialTypeCode[] SerialTypeCodes { get; private set; }
    public Column[] Columns { get; private set; }

    private SerialTypeCode[] GetSerialTypeCodes(Stream recordStream)
    {
        var serialTypeCodes = new List<SerialTypeCode>();
        while (recordStream.Position < HeaderSize)
        {
            var serialTypeValue = (byte)recordStream.ReadVarint();
            SerialTypeCode serialTypeCode = new(serialTypeValue);
            serialTypeCodes.Add(serialTypeCode);
        }

        return serialTypeCodes.ToArray();
    }

    private Column[] GetColumns(Stream recordStream)
    {
        if (SerialTypeCodes.Length == 0)
            throw new InvalidOperationException("No serial types found in the record header");

        if (recordStream.Position >= recordStream.Length)
            throw new InvalidOperationException("No columns found in the record body");

        return SerialTypeCodes.Select(serialTypeCode => new Column(serialTypeCode, recordStream)).ToArray();
    }
}