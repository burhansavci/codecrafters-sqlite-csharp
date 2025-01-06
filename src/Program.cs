using codecrafters_sqlite.Sqlite;
using codecrafters_sqlite.Sqlite.Queries;

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
    var db = new Database(databaseFile);
    Console.WriteLine($"database page size: {db.SchemaPage.DbHeader!.PageSize}");
    Console.WriteLine($"number of tables: {db.SchemaPage.Header.NumberOfCells}");
}
else if (command == ".tables")
{
    var db = new Database(databaseFile);

    foreach (var cell in db.SchemaPage.Cells)
    {
        var tableName = cell.Record!.Columns[2].Value!.ToString();

        if (tableName == "sqlite_sequence")
            continue;

        Console.Write(tableName + " ");
    }
}
else if (command.StartsWith("SELECT COUNT(*) FROM", StringComparison.InvariantCultureIgnoreCase))
{
    var tableName = command.Split(" ")[3];

    var db = new Database(databaseFile);

    var schemaCell = db.GetSchemaCell(tableName);

    var rootPageNumber = (byte)schemaCell.Record!.Columns[3].Value!;

    var page = db.GetPage(rootPageNumber);

    Console.WriteLine(page.Header.NumberOfCells);
}
else if (command.StartsWith("SELECT", StringComparison.InvariantCultureIgnoreCase))
{
    var db = new Database(databaseFile);

    var query = new SqlQuery(command);

    var result = db.ExecuteQuery(query);

    Console.WriteLine(result);
}
else
{
    throw new InvalidOperationException($"Invalid command: {command}");
}