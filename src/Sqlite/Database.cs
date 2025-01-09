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

        SchemaPage = new SchemaPage(new Page(_databaseFileStream));
    }

    public SchemaPage SchemaPage { get; }

    public SqlQueryResult ExecuteQuery(SqlQuery query)
    {
        ArgumentNullException.ThrowIfNull(query);

        var tableEntry = SchemaPage.GetTableEntry(query.TableName);
        if (tableEntry is null)
            throw new InvalidOperationException($"Table {query.TableName} not found");

        query.ApplyTableSchema(tableEntry);

        if (query.Where == null)
            return DoFullTableScan(query, tableEntry);

        var indexEntry = SchemaPage.GetIndexEntry(query.TableName);
        if (query.TryApplyIndexSchema(indexEntry))
        {
            var matchingRowIds = DoIndexSeek(query, indexEntry!);

            return DoKeyLookup(query, tableEntry, matchingRowIds);
        }

        // If no index is available, do a full table scan
        return DoFullTableScan(query, tableEntry);
    }

    public Page GetPage(int pageNumber)
    {
        var pageBytes = new byte[SchemaPage.DbHeader.PageSize];

        _databaseFileStream.Seek((pageNumber - 1) * SchemaPage.DbHeader.PageSize, SeekOrigin.Begin);
        _databaseFileStream.ReadExactly(pageBytes, 0, SchemaPage.DbHeader.PageSize);

        using var pageStream = new MemoryStream(pageBytes);
        return new Page(pageStream);
    }

    private long[] DoIndexSeek(SqlQuery query, SchemaEntry indexEntry)
    {
        var indexRootPage = GetPage(indexEntry.RootPage);

        var indexCells = indexRootPage.Header.PageType switch
        {
            PageType.LeafIndex => query.GetResult(indexRootPage),
            PageType.InteriorIndex => TraversePages(indexRootPage, query),
            _ => throw new InvalidOperationException("Unexpected header page type")
        };

        var matchingRowIds = indexCells.Select(cell => Convert.ToInt64(cell.Record!.Columns[1].Value!)).ToArray();
        return matchingRowIds;
    }

    private SqlQueryResult DoKeyLookup(SqlQuery query, SchemaEntry tableEntry, long[] matchingRowIds)
    {
        var tableRootPage = GetPage(tableEntry.RootPage);

        var matchingCells = tableRootPage.Header.PageType switch
        {
            PageType.LeafTable => query.GetResult(tableRootPage),
            PageType.InteriorTable => TraverseTablePages(tableRootPage, matchingRowIds),
            _ => throw new InvalidOperationException("Unexpected header page type")
        };

        return new SqlQueryResult(matchingCells.ToArray(), query);
    }

    private SqlQueryResult DoFullTableScan(SqlQuery query, SchemaEntry tableEntry)
    {
        var rootPage = GetPage(tableEntry.RootPage);
        var cells = rootPage.Header.PageType switch
        {
            PageType.LeafTable => query.GetResult(rootPage),
            PageType.InteriorTable => TraversePages(rootPage, query),
            _ => throw new InvalidOperationException("Unexpected header page type")
        };

        return new SqlQueryResult(cells.ToArray(), query);
    }

    private IEnumerable<Cell> TraversePages(Page rootPage, SqlQuery query)
    {
        if (rootPage.Header.PageType is PageType.LeafTable or PageType.LeafIndex or PageType.InteriorIndex)
        {
            foreach (var cell in query.GetResult(rootPage))
                yield return cell;
        }

        if (rootPage.Header.PageType is PageType.InteriorTable or PageType.InteriorIndex)
        {
            foreach (var cell in rootPage.Cells)
            {
                var childPage = GetPage((int)cell.LeftChildPageNumber!.Value);
                foreach (var childCell in TraversePages(childPage, query))
                    yield return childCell;
            }

            if (rootPage.Header.RightMostPointer.HasValue)
            {
                var rightMostPage = GetPage((int)rootPage.Header.RightMostPointer.Value);
                foreach (var rightMostCell in TraversePages(rightMostPage, query))
                    yield return rightMostCell;
            }
        }
    }

    private IEnumerable<Cell> TraverseTablePages(Page page, long[] matchingRowIds)
    {
        if (matchingRowIds.Length == 0)
            yield break;

        foreach (var rowId in matchingRowIds)
        {
            var cell = FindCellByRowId(page, rowId);
            if (cell != null)
                yield return cell;
        }
    }

    private Cell? FindCellByRowId(Page page, long targetRowId)
    {
        if (page.Header.PageType == PageType.LeafTable)
            return page.Cells.FirstOrDefault(cell => cell.RowId.HasValue && cell.RowId.Value == targetRowId);

        if (page.Header.PageType != PageType.InteriorTable)
            return null;

        // Binary search through interior cells to find the right path
        foreach (var cell in page.Cells)
        {
            if (cell.RowId >= targetRowId)
            {
                var childPage = GetPage((int)cell.LeftChildPageNumber!.Value);
                return FindCellByRowId(childPage, targetRowId);
            }
        }

        // If we haven't found the right path yet, check the rightmost pointer
        if (page.Header.RightMostPointer.HasValue)
        {
            var rightMostPage = GetPage((int)page.Header.RightMostPointer.Value);
            return FindCellByRowId(rightMostPage, targetRowId);
        }

        return null;
    }
}