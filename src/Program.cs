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
    databaseFile.Seek(16, SeekOrigin.Begin); // Skip the first 16 bytes
    byte[] pageSizeBytes = new byte[2];
    databaseFile.ReadExactly(pageSizeBytes, 0, 2);
    var pageSize = ReadUInt16BigEndian(pageSizeBytes);
    Console.WriteLine($"database page size: {pageSize}");

    databaseFile.Seek(100, SeekOrigin.Begin); // Skip The Database Header

    databaseFile.Seek(3, SeekOrigin.Current); // Skip the first 3 bytes
    byte[] numberOfTablesBytes = new byte[2];
    databaseFile.ReadExactly(numberOfTablesBytes, 0, 2);
    var numberOfTables = ReadUInt16BigEndian(numberOfTablesBytes);
    Console.WriteLine($"number of tables: {numberOfTables}");
}
else
{
    throw new InvalidOperationException($"Invalid command: {command}");
}
