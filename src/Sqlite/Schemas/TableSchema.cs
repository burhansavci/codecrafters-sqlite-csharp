namespace codecrafters_sqlite.Sqlite.Schemas;

public record TableSchema
{
    private readonly ColumnSchema[] _columns;

    public TableSchema(string createSql)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(createSql);

        _columns = ParseCreateTableColumns(createSql);
    }

    public ColumnSchema GetColumn(string columnName)
    {
        var column = _columns.FirstOrDefault(c => string.Equals(c.Name, columnName, StringComparison.CurrentCultureIgnoreCase));

        if (column == null)
            throw new InvalidOperationException($"Column {columnName} not found in table schema");

        return column;
    }

    private static ColumnSchema[] ParseCreateTableColumns(string createSql)
    {
        var columnsSection = createSql.Split("(")[1];
        columnsSection = columnsSection[..columnsSection.LastIndexOf(')')];

        return columnsSection
            .Split(",")
            .Select((column, index) =>
            {
                var parts = column.Trim().Split(" ", 2);
                return new ColumnSchema(parts, index);
            })
            .ToArray();
    }
}