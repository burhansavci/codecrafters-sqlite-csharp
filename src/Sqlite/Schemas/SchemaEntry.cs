using codecrafters_sqlite.Sqlite.Pages;

namespace codecrafters_sqlite.Sqlite.Schemas;

public record SchemaEntry
{
    public SchemaEntry(Cell cell)
    {
        ArgumentNullException.ThrowIfNull(cell);
        ArgumentNullException.ThrowIfNull(cell.Record);

        Type = cell.Record.Columns[0].Value!.ToString()!;
        Name = cell.Record.Columns[1].Value!.ToString()!;
        TableName = cell.Record.Columns[2].Value!.ToString()!;
        RootPage = Convert.ToInt32(cell.Record.Columns[3].Value!);
        Sql = cell.Record.Columns[4].Value?.ToString();

        switch (Type)
        {
            case "table" when Sql != null:
            {
                var columnsSection = Sql.Split("(")[1];
                columnsSection = columnsSection[..columnsSection.LastIndexOf(')')];

                Columns = columnsSection
                    .Split(",")
                    .Select((column, index) =>
                    {
                        var parts = column.Trim().Split(" ", 2);
                        return new ColumnInfo(parts[0], parts.Length > 1 ? parts[1] : string.Empty, index);
                    })
                    .ToArray();
                break;
            }
            case "index" when Sql != null:
            {
                var sql = string.Join(" ", Sql.Split(['\n', '\t', ' '], StringSplitOptions.RemoveEmptyEntries));
                var columnName = sql.Split('(')[1].TrimEnd(')');
                Columns = [new ColumnInfo(columnName, string.Empty, 0)];
                break;
            }
            default:
            {
                throw new InvalidOperationException($"Unsupported schema type: {Type}");
            }
        }
    }

    /// <summary>
    /// The type of the object: 'table', 'index', 'view', or 'trigger'
    /// </summary>
    public string Type { get; }

    public string Name { get; }

    public string TableName { get; }

    public int RootPage { get; }

    public string? Sql { get; }

    public ColumnInfo[] Columns { get; }
}