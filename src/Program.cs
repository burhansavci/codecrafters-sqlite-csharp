using System.Text;
using static System.Buffers.Binary.BinaryPrimitives;


// Each byte contributes 7 bits of data
const byte VarintEncodingBits = 7;

// The upper bit indicates whether there are more bytes
const byte VarintContinueFlag = 1 << VarintEncodingBits;

// Parse arguments
var (path, command) = args.Length switch
{
    0 => throw new InvalidOperationException("Missing <database path> and <command>"),
    1 => throw new InvalidOperationException("Missing <command>"),
    _ => (args[0], args[1])
};

var databaseFile = File.OpenRead(path);

// Parse command and act accordingly
if (command == ".dbinfo")
{
    databaseFile.Seek(16, SeekOrigin.Begin); // Skip the first 16 bytes
    byte[] pageSizeBytes = new byte[2];
    databaseFile.ReadExactly(pageSizeBytes, 0, 2);
    var pageSize = ReadUInt16BigEndian(pageSizeBytes);
    Console.WriteLine($"database page size: {pageSize}");

    databaseFile.Seek(100, SeekOrigin.Begin); // Skip The Database Header

    databaseFile.Seek(3, SeekOrigin.Current); // Skip the first 3 bytes
    byte[] numberOfCellsBytes = new byte[2];
    databaseFile.ReadExactly(numberOfCellsBytes, 0, 2);
    var numberOfCells = ReadUInt16BigEndian(numberOfCellsBytes);
    Console.WriteLine($"number of tables: {numberOfCells}");
}
else if (command == ".tables")
{
    databaseFile.Seek(100, SeekOrigin.Begin); // Skip The Database Header

    // Read the B-tree Page Header
    var btreePageType = (PageType)databaseFile.ReadByte();

    databaseFile.Seek(2, SeekOrigin.Current); // Skip the freeblock offset
    byte[] numberOfCellsBytes = new byte[2];
    databaseFile.ReadExactly(numberOfCellsBytes, 0, 2);
    var numberOfCells = ReadUInt16BigEndian(numberOfCellsBytes);

    databaseFile.Seek(3, SeekOrigin.Current); // Skip the remaining of the B-tree Page Header

    if (btreePageType is PageType.InteriorTable or PageType.InteriorIndex)
    {
        //The four-byte page number at offset 8 is the right-most pointer. This value appears in the header of interior b-tree pages only and is omitted from all other pages.
        databaseFile.Seek(4, SeekOrigin.Current); // Skip the right-most pointer of the interior b-tree page header
    }

    // Read the cell pointer array
    ushort[] cellPointers = new ushort[numberOfCells];
    for (int i = 0; i < numberOfCells; i++)
    {
        byte[] cellPointerBytes = new byte[2];
        databaseFile.ReadExactly(cellPointerBytes, 0, 2);
        cellPointers[i] = ReadUInt16BigEndian(cellPointerBytes);
    }

    // Read the cell records
    for (int i = 0; i < numberOfCells; i++)
    {
        /*
         1. The size of the record, in bytes (varint)
         2. The rowid (varint)
         3. The record (record format)
         */

        databaseFile.Seek(cellPointers[i], SeekOrigin.Begin);

        // Read the size of the record
        var recordSizeInBytes = ReadVarintByteUntilFinished(databaseFile);

        // Read the rowid
        var rowid = ReadVarintByteUntilFinished(databaseFile);

        // Read the record in record format
        /*
         Header:
           Size of the header, including this value (varint)
           Serial type code for each column in the record, in order (varint)
         Body:
           The value of each column in the record, in order (format varies based on serial type code)
         */
        var record = new byte[recordSizeInBytes];
        databaseFile.ReadExactly(record, 0, recordSizeInBytes);

        using var memoryStream = new MemoryStream(record);
        // Read the header of the record
        var recordHeaderSize = ReadVarintByteUntilFinished(memoryStream);
        var serialTypes = new List<byte>();
        while (memoryStream.Position < recordHeaderSize)
        {
            var serialType = (byte)ReadVarintByteUntilFinished(memoryStream);
            serialTypes.Add(serialType);
        }

        // Read the body of the record
        var recordBody = new byte[recordSizeInBytes - recordHeaderSize];
        memoryStream.ReadExactly(recordBody, 0, recordSizeInBytes - recordHeaderSize);

        using var bodyStream = new MemoryStream(recordBody);
        List<string> columnValues = new(serialTypes.Count);
        foreach (var serialType in serialTypes)
        {
            if (serialType >= 13 && serialType % 2 == 1)
            {
                // Value is a string in the text encoding and (N-13)/2 bytes in length. The nul terminator is not stored.
                var length = (serialType - 13) / 2;
                var value = new byte[length];
                bodyStream.ReadExactly(value, 0, length);
                var valueString = Encoding.UTF8.GetString(value);
                columnValues.Add(valueString);
            }
            else if (serialType == 1)
            {
                // Value is an 8-bit twos-complement integer.
                var value = bodyStream.ReadByte();
                columnValues.Add(value.ToString());
            }
        }

        var tableName = columnValues[2];
        if (tableName == "sqlite_sequence")
            continue;

        Console.Write(tableName + " ");
    }
}
else
{
    throw new InvalidOperationException($"Invalid command: {command}");
}


static int ReadVarintByteUntilFinished(Stream stream)
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

static (byte Value, bool MoreBytes) ReadVarintByte(Stream stream)
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

public enum PageType : byte
{
    /*
       The one-byte flag at offset 0 indicating the b-tree page type.

       A value of 2 (0x02) means the page is an interior index b-tree page.
       A value of 5 (0x05) means the page is an interior table b-tree page.
       A value of 10 (0x0a) means the page is a leaf index b-tree page.
       A value of 13 (0x0d) means the page is a leaf table b-tree page.

       Any other value for the b-tree page type is an error.
    */
    InteriorIndex = 0x02,
    InteriorTable = 0x05,
    LeafIndex = 0x0A,
    LeafTable = 0x0D
}