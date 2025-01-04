using System.Text;
using codecrafters_sqlite.Sqlite;
using codecrafters_sqlite.Sqlite.Extensions;
using codecrafters_sqlite.Sqlite.Pages;
using static System.Buffers.Binary.BinaryPrimitives;

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
    var dbHeader = new DbHeader(databaseFile);
    Console.WriteLine($"database page size: {dbHeader.PageSize}");

    databaseFile.Seek(3, SeekOrigin.Current); // Skip the first 3 bytes
    byte[] numberOfCellsBytes = new byte[2];
    databaseFile.ReadExactly(numberOfCellsBytes, 0, 2);
    var numberOfCells = ReadUInt16BigEndian(numberOfCellsBytes);
    Console.WriteLine($"number of tables: {numberOfCells}");
}
else if (command == ".tables")
{
    _ = new DbHeader(databaseFile); // Skip The Database Header

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
        var recordSizeInBytes = databaseFile.ReadVarint();

        // Read the rowid
        var rowid = databaseFile.ReadVarint();

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
        var recordHeaderSize = memoryStream.ReadVarint();
        var serialTypes = new List<byte>();
        while (memoryStream.Position < recordHeaderSize)
        {
            var serialType = (byte)memoryStream.ReadVarint();
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
else if (command.StartsWith("SELECT COUNT(*) FROM", StringComparison.CurrentCultureIgnoreCase))
{
    var tableName = command.Split(" ")[3];

    var dbHeader = new DbHeader(databaseFile);

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
    int rootPage = 0;
    for (int i = 0; i < numberOfCells; i++)
    {
        /*
         1. The size of the record, in bytes (varint)
         2. The rowid (varint)
         3. The record (record format)
         */
        databaseFile.Seek(cellPointers[i], SeekOrigin.Begin);

        // Read the size of the record
        var recordSizeInBytes = databaseFile.ReadVarint();

        // Read the rowid
        var rowid = databaseFile.ReadVarint();

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
        var recordHeaderSize = memoryStream.ReadVarint();
        var serialTypes = new List<byte>();
        while (memoryStream.Position < recordHeaderSize)
        {
            var serialType = (byte)memoryStream.ReadVarint();
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

        var tableNameFromColumn = columnValues[2];
        if (tableNameFromColumn == tableName)
        {
            rootPage = int.Parse(columnValues[3]);
            break;
        }
    }

    // Read the root page
    databaseFile.Seek((rootPage - 1) * dbHeader.PageSize, SeekOrigin.Begin);

    var rootPageType = (PageType)databaseFile.ReadByte();

    databaseFile.Seek(2, SeekOrigin.Current); // Skip the freeblock offset
    byte[] numberOfCellsBytesPage = new byte[2];
    databaseFile.ReadExactly(numberOfCellsBytesPage, 0, 2);
    var numberOfCellsPage = ReadUInt16BigEndian(numberOfCellsBytesPage);

    Console.WriteLine(numberOfCellsPage);
}
else
{
    throw new InvalidOperationException($"Invalid command: {command}");
}