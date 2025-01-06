using codecrafters_sqlite.Sqlite.Pages;
using codecrafters_sqlite.Sqlite.Queries;
using codecrafters_sqlite.Sqlite.Schemas;

namespace codecrafters_sqlite.Sqlite;

public record Database
{
    private readonly FileStream _databaseFileStream;

    public Database(FileStream databaseFileStream)
    {
        ArgumentNullException.ThrowIfNull(databaseFileStream);

        if (databaseFileStream.CanRead == false)
            throw new ArgumentException("The stream must be readable", nameof(databaseFileStream));

        _databaseFileStream = databaseFileStream;

        SchemaPage = new Page(_databaseFileStream);
    }

    public Page SchemaPage { get; }

    public SqlQueryResult ExecuteQuery(SqlQuery query)
    {
        var tableSchemaCell = GetSchemaCell(query.TableName);

        var tableSchema = GetTableSchema(tableSchemaCell);
        query.ApplySchema(tableSchema);

        var rootPageNumber = (byte)tableSchemaCell.Record!.Columns[3].Value!;
        Page rootPage = GetPage(rootPageNumber);

        var cells = rootPage.Header.PageType switch
        {
            PageType.LeafTable => query.GetResult(rootPage),
            PageType.InteriorTable => TraversePages(rootPage, query),
            _ => throw new InvalidOperationException("Unexpected header page type")
        };

        return new SqlQueryResult(cells, query);
    }

    public Cell GetSchemaCell(string tableName) => SchemaPage.Cells.First(cell => cell.Record!.Columns[2].Value!.ToString() == tableName);

    private static TableSchema GetTableSchema(Cell tableSchemaCell)
    {
        var createSql = tableSchemaCell.Record!.Columns[^1].Value!.ToString()!;

        return new TableSchema(createSql);
    }

    public Page GetPage(int pageNumber)
    {
        var pageBytes = new byte[SchemaPage.DbHeader!.PageSize];

        _databaseFileStream.Seek((pageNumber - 1) * SchemaPage.DbHeader!.PageSize, SeekOrigin.Begin);
        _databaseFileStream.ReadExactly(pageBytes, 0, SchemaPage.DbHeader.PageSize);

        using var pageStream = new MemoryStream(pageBytes);
        return new Page(pageStream);
    }

    private Cell[] TraversePages(Page rootPage, SqlQuery query)
    {
        List<Cell> cells = [];

        for (var i = 0; i <= rootPage.Cells.Length; i++)
        {
            var pageNumber = i < rootPage.Cells.Length
                ? rootPage.Cells[i].LeftChildPageNumber!.Value // For all but the last iteration, use the left child page number from the current cell
                : rootPage.Header.RightMostPointer; // For the last iteration, use the rightmost child page number from the page header

            var childPage = GetPage((int)pageNumber);

            if (childPage.Header.PageType == PageType.LeafTable)
            {
                cells.AddRange(query.GetResult(childPage));
            }
            else if (childPage.Header.PageType == PageType.InteriorTable)
            {
                cells.AddRange(TraversePages(childPage, query));
            }
        }

        return cells.ToArray();
    }
}