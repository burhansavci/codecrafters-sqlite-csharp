using codecrafters_sqlite.Sqlite;
using codecrafters_sqlite.Sqlite.Pages;
using codecrafters_sqlite.Sqlite.Schemas;

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
    var schemaPage = new Page(databaseFile);
    Console.WriteLine($"database page size: {schemaPage.DbHeader!.PageSize}");
    Console.WriteLine($"number of tables: {schemaPage.Header.NumberOfCells}");
}
else if (command == ".tables")
{
    var schemaPage = new Page(databaseFile);

    foreach (var cell in schemaPage.Cells)
    {
        var tableName = cell.Record.Columns[2].Value!.ToString();

        if (tableName == "sqlite_sequence")
            continue;

        Console.Write(tableName + " ");
    }
}
else if (command.StartsWith("SELECT COUNT(*) FROM", StringComparison.InvariantCultureIgnoreCase))
{
    var tableName = command.Split(" ")[3];

    var schemaPage = new Page(databaseFile);

    var schemaCell = schemaPage.Cells.First(cell => cell.Record.Columns[2].Value!.ToString() == tableName);

    var rootPage = (byte)schemaCell.Record.Columns[3].Value!;

    databaseFile.Seek((rootPage - 1) * schemaPage.DbHeader!.PageSize, SeekOrigin.Begin);
    var pageHeader = new PageHeader(databaseFile);

    Console.WriteLine(pageHeader.NumberOfCells);
}
else if (command.StartsWith("SELECT", StringComparison.InvariantCultureIgnoreCase))
{
    var query = new SqlQuery(command);

    var schemaPage = new Page(databaseFile);
    var schemaCell = schemaPage.Cells.First(cell => cell.Record.Columns[2].Value!.ToString() == query.TableName);

    var createSql = schemaCell.Record.Columns[^1].Value!.ToString()!;
    query.ApplySchema(new TableSchema(createSql));

    var rootPageNumber = (byte)schemaCell.Record.Columns[3].Value!;
    var rootPageBytes = new byte[schemaPage.DbHeader!.PageSize];
    databaseFile.Seek((rootPageNumber - 1) * schemaPage.DbHeader!.PageSize, SeekOrigin.Begin);
    databaseFile.ReadExactly(rootPageBytes, 0, schemaPage.DbHeader.PageSize);
    using var rootPageStream = new MemoryStream(rootPageBytes);
    var rootPage = new Page(rootPageStream);

    Cell[] cells = [];
    if (rootPage.Header.PageType == PageType.LeafTable)
    {
        cells = query.GetFilteredCells(rootPage);
    }
    else if (rootPage.Header.PageType == PageType.InteriorTable)
    {
        cells = GetCells(rootPage, schemaPage, query);
    }

    foreach (var cell in cells)
    {
        for (int i = 0; i < query.Columns!.Length; i++)
        {
            var queryColumn = query.Columns[i];
            var recordColumn = cell.Record.Columns[queryColumn.Index];

            if (queryColumn.IsRowIdAlias && recordColumn.Value is null)
            {
                Console.Write(cell.RowId);
            }
            else
            {
                Console.Write(recordColumn.Value);
            }

            if (i < query.Columns.Length - 1)
                Console.Write("|");
        }

        Console.WriteLine();
    }
}
else
{
    throw new InvalidOperationException($"Invalid command: {command}");
}

Cell[] GetCells(Page rootPage, Page schemaPage, SqlQuery query)
{
    List<Cell> cells = [];

    for (var i = 0; i <= rootPage.Cells.Length; i++)
    {
        uint pageNumber;
        if (i < rootPage.Cells.Length)
        {
            // For all but the last iteration, use the left child page number from the current cell
            pageNumber = rootPage.Cells[i].LeftChildPageNumber!.Value;
        }
        else
        {
            // For the last iteration, use the rightmost child page number from the page header
            pageNumber = rootPage.Header.RightMostPointer;
        }

        var childPageBytes = new byte[schemaPage.DbHeader!.PageSize];
        databaseFile.Seek((pageNumber - 1) * schemaPage.DbHeader!.PageSize, SeekOrigin.Begin);
        databaseFile.ReadExactly(childPageBytes, 0, schemaPage.DbHeader.PageSize);

        using var childPageStream = new MemoryStream(childPageBytes);
        var childPage = new Page(childPageStream);

        if (childPage.Header.PageType == PageType.LeafTable)
        {
            cells.AddRange(query.GetFilteredCells(childPage));
        }
        else if (childPage.Header.PageType == PageType.InteriorTable)
        {
            cells.AddRange(GetCells(childPage, schemaPage, query));
        }
    }

    return cells.ToArray();
}