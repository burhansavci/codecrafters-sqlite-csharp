using System.Buffers.Binary;
using codecrafters_sqlite.Sqlite.Extensions;

namespace codecrafters_sqlite.Sqlite.Pages;

// B-tree Page Header Format: https://www.sqlite.org/fileformat.html#b_tree_pages
public record Page
{
    public Page(FileStream databaseFileStream)
    {
        ArgumentNullException.ThrowIfNull(databaseFileStream, nameof(databaseFileStream));

        if (databaseFileStream.CanRead == false)
            throw new ArgumentException("The stream must be readable", nameof(databaseFileStream));

        DbHeader = new DbHeader(databaseFileStream);
        Header = new PageHeader(databaseFileStream);
        CellPointers = GetCellPointers(databaseFileStream);
        Cells = GetCells(databaseFileStream);
    }

    public Page(Stream pageStream)
    {
        ArgumentNullException.ThrowIfNull(pageStream, nameof(pageStream));

        if (pageStream.CanRead == false)
            throw new ArgumentException("The stream must be readable", nameof(pageStream));

        Header = new PageHeader(pageStream);
        CellPointers = GetCellPointers(pageStream);
        Cells = GetCells(pageStream);
    }

    public DbHeader? DbHeader { get; private init; }
    public PageHeader Header { get; }
    public Cell[] Cells { get; private set; }
    private ushort[] CellPointers { get; }

    private ushort[] GetCellPointers(Stream pageStream)
    {
        var cellPointers = new ushort[Header.NumberOfCells];
        for (var i = 0; i < Header.NumberOfCells; i++)
        {
            var cellPointerBytes = new byte[2];
            pageStream.ReadExactly(cellPointerBytes, 0, 2);
            cellPointers[i] = BinaryPrimitives.ReadUInt16BigEndian(cellPointerBytes);
        }

        return cellPointers;
    }

    private Cell[] GetCells(Stream pageStream)
    {
        if (CellPointers.Length == 0)
            return [];

        var cells = new Cell[Header.NumberOfCells];
        for (var i = 0; i < Header.NumberOfCells; i++)
        {
            pageStream.Seek(CellPointers[i], SeekOrigin.Begin);

            if (Header.PageType == PageType.InteriorTable)
            {
                var leftChildPageNumber = pageStream.ReadUInt32BigEndian();
                var rowId = pageStream.ReadVarint();

                cells[i] = new Cell(leftChildPageNumber, null, rowId, null, null);
            }
            else if (Header.PageType == PageType.LeafTable)
            {
                var size = pageStream.ReadVarint();
                var rowId = pageStream.ReadVarint();

                var recordBytes = new byte[size];
                pageStream.ReadExactly(recordBytes, 0, (int)size);

                using var recordStream = new MemoryStream(recordBytes);
                var record = new Record(recordStream);

                cells[i] = new Cell(null, size, rowId, record, null);
            }
        }

        return cells;
    }
}