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
    Console.WriteLine($"database page size: {db.SchemaPage.DbHeader.PageSize}");
    Console.WriteLine($"number of tables: {db.SchemaPage.Header.NumberOfCells}");
}
else if (command == ".tables")
{
    var db = new Database(databaseFile);

    foreach (var table in db.SchemaPage.Tables)
    {
        Console.Write(table.Name + " ");
    }
}
else if (command.StartsWith("SELECT COUNT(*) FROM", StringComparison.InvariantCultureIgnoreCase))
{
    var tableName = command.Split(" ")[3];

    var db = new Database(databaseFile);

    var tableEntry = db.SchemaPage.GetTableEntry(tableName);

    var page = db.GetPage(tableEntry!.RootPage);

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