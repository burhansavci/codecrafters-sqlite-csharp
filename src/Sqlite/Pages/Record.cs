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

    public long HeaderSize { get; }
    public SerialTypeCode[] SerialTypeCodes { get; }
    public Column[] Columns { get; private set; }

    private SerialTypeCode[] GetSerialTypeCodes(Stream recordStream)
    {
        var serialTypeCodes = new List<SerialTypeCode>();
        while (recordStream.Position < HeaderSize)
        {
            var serialTypeValue = recordStream.ReadVarint();
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

        return SerialTypeCodes.Select((serialTypeCode, index) => new Column(serialTypeCode, index, recordStream)).ToArray();
    }
}