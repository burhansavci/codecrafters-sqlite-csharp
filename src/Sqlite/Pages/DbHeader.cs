using codecrafters_sqlite.Sqlite.Extensions;

namespace codecrafters_sqlite.Sqlite.Pages;

// Database Header Format: https://www.sqlite.org/fileformat.html#the_database_header
public record DbHeader
{
    public DbHeader(FileStream databaseFileStream)
    {
        ArgumentNullException.ThrowIfNull(databaseFileStream, nameof(databaseFileStream));

        if (databaseFileStream.CanRead == false)
            throw new ArgumentException("The stream must be readable", nameof(databaseFileStream));

        if (databaseFileStream.Position != 0)
            throw new ArgumentException("The stream must be at the beginning to read db header", nameof(databaseFileStream));

        if (databaseFileStream.Length < 100)
            throw new ArgumentException("The stream must be at least 100 bytes long", nameof(databaseFileStream));

        Header = databaseFileStream.ReadString(16);
        PageSize = databaseFileStream.ReadUInt16BigEndian();
        FileFormatWriteVersion = databaseFileStream.ReadByte();
        FileFormatReadVersion = databaseFileStream.ReadByte();
        ReservedSpace = databaseFileStream.ReadByte();
        MaximumEmbeddedPayloadFraction = databaseFileStream.ReadByte();
        MinimumEmbeddedPayloadFraction = databaseFileStream.ReadByte();
        LeafPayloadFraction = databaseFileStream.ReadByte();
        FileChangeCounter = databaseFileStream.ReadUInt32BigEndian();
        DatabaseFileSize = databaseFileStream.ReadUInt32BigEndian();
        FirstFreelistTrunkPage = databaseFileStream.ReadUInt32BigEndian();
        TotalNumberOfFreelistPages = databaseFileStream.ReadUInt32BigEndian();
        SchemaCookie = databaseFileStream.ReadUInt32BigEndian();
        SchemaFormatNumber = databaseFileStream.ReadUInt32BigEndian();
        DefaultPageCacheSize = databaseFileStream.ReadUInt32BigEndian();
        LargestRootBTreePageNumber = databaseFileStream.ReadUInt32BigEndian();
        DatabaseTextEncoding = databaseFileStream.ReadUInt32BigEndian();
        UserVersion = databaseFileStream.ReadUInt32BigEndian();
        IncrementalVacuumMode = databaseFileStream.ReadUInt32BigEndian();
        ApplicationId = databaseFileStream.ReadUInt32BigEndian();
        ReservedForExpansion = databaseFileStream.ReadBytes(20);
        VersionValidForNumber = databaseFileStream.ReadUInt32BigEndian();
        SqliteVersionNumber = databaseFileStream.ReadUInt32BigEndian();
    }

    public string Header { get; private init; }
    public ushort PageSize { get; private init; }
    public int FileFormatWriteVersion { get; private init; }
    public int FileFormatReadVersion { get; private init; }
    public int ReservedSpace { get; private init; }
    public int MaximumEmbeddedPayloadFraction { get; private init; }
    public int MinimumEmbeddedPayloadFraction { get; private init; }
    public int LeafPayloadFraction { get; private init; }
    public uint FileChangeCounter { get; private init; }
    public uint DatabaseFileSize { get; private init; }
    public uint FirstFreelistTrunkPage { get; private init; }
    public uint TotalNumberOfFreelistPages { get; private init; }
    public uint SchemaCookie { get; private init; }
    public uint SchemaFormatNumber { get; private init; }
    public uint DefaultPageCacheSize { get; private init; }
    public uint LargestRootBTreePageNumber { get; private init; }
    public uint DatabaseTextEncoding { get; private init; }
    public uint UserVersion { get; private init; }
    public uint IncrementalVacuumMode { get; private init; }
    public uint ApplicationId { get; private init; }
    public byte[] ReservedForExpansion { get; private init; }
    public uint VersionValidForNumber { get; private init; }
    public uint SqliteVersionNumber { get; private init; }
}