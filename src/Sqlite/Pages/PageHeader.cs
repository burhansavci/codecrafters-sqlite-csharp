using codecrafters_sqlite.Sqlite.Extensions;

namespace codecrafters_sqlite.Sqlite.Pages;

// B-tree Page Header Format: https://www.sqlite.org/fileformat.html#b_tree_pages
public record PageHeader
{
    public PageHeader(Stream databaseFileStream)
    {
        ArgumentNullException.ThrowIfNull(databaseFileStream, nameof(databaseFileStream));

        if (databaseFileStream.CanRead == false)
            throw new ArgumentException("The stream must be readable", nameof(databaseFileStream));

        PageType = (PageType)databaseFileStream.ReadByte();
        FirstFreeblockOffset = databaseFileStream.ReadUInt16BigEndian();
        NumberOfCells = databaseFileStream.ReadUInt16BigEndian();
        StartOfCellContentArea = databaseFileStream.ReadUInt16BigEndian();
        FragmentedFreeBytes = (byte)databaseFileStream.ReadByte();

        //The four-byte page number at offset 8 is the right-most pointer.
        //This value appears in the header of interior b-tree pages only and is omitted from all other pages.
        if (PageType is PageType.InteriorTable or PageType.InteriorIndex) 
            RightMostPointer = databaseFileStream.ReadUInt32BigEndian();
    }
    public PageType PageType { get; }
    public ushort FirstFreeblockOffset { get; private init; }
    public ushort NumberOfCells { get; private init; }
    public ushort StartOfCellContentArea { get; private init; }
    public byte FragmentedFreeBytes { get; private init; }
    public uint RightMostPointer { get; private init; }
}