using codecrafters_sqlite.Sqlite.Pages;

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
    var page = new Page(databaseFile);

    foreach (var cell in page.Cells)
    {
        var tableName = cell.Record.Columns[2].Value!.ToString();

        if (tableName == "sqlite_sequence")
            continue;

        Console.Write(tableName + " ");
    }
}
else if (command.StartsWith("SELECT COUNT(*) FROM", StringComparison.CurrentCultureIgnoreCase))
{
    var tableName = command.Split(" ")[3];

    var schemaPage = new Page(databaseFile);

    var schemaCell = schemaPage.Cells.First(cell => cell.Record.Columns[2].Value!.ToString() == tableName);

    var rootPage = (byte)schemaCell.Record.Columns[3].Value!;
    
    databaseFile.Seek((rootPage - 1) * schemaPage.DbHeader!.PageSize, SeekOrigin.Begin);
    var pageHeader = new PageHeader(databaseFile);
    
    Console.WriteLine(pageHeader.NumberOfCells);
}
else
{
    throw new InvalidOperationException($"Invalid command: {command}");
}