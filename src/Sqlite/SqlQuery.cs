using codecrafters_sqlite.Sqlite.Pages;
using codecrafters_sqlite.Sqlite.Schemas;

namespace codecrafters_sqlite.Sqlite;

public record SqlQuery
{
    private readonly string[] _columnNames;

    public SqlQuery(string query)
    {
        ArgumentNullException.ThrowIfNull(query);
        query = query.ToLowerInvariant();

        var parts = query.Split("from", StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length != 2)
            throw new ArgumentException("Invalid SQL query format", nameof(query));

        _columnNames = ParseSelectColumnNames(parts[0]);
        (TableName, WhereColumnName, WhereValue) = ParseFromClause(parts[1]);
    }

    public (string Name, int Index)[]? Columns { get; private set; }
    public string TableName { get; }
    public string? WhereColumnName { get; }
    public string? WhereValue { get; }
    public int? WhereColumnIndex { get; private set; }

    public void ApplySchema(TableSchema schema)
    {
        Columns = new (string Name, int Index)[_columnNames.Length];

        for (var i = 0; i < _columnNames.Length; i++)
        {
            var column = schema.GetColumn(_columnNames[i]);
            Columns[i] = (column.Name, column.Index);

            if (string.Equals(WhereColumnName, column.Name, StringComparison.CurrentCultureIgnoreCase))
                WhereColumnIndex = column.Index;
        }

        // Handle case where WHERE column wasn't in selected columns
        if (WhereColumnName != null && WhereColumnIndex == null)
            WhereColumnIndex = schema.GetColumn(WhereColumnName).Index;
    }

    public Cell[] GetFilteredCells(Page page) =>
        string.IsNullOrWhiteSpace(WhereColumnName)
            ? page.Cells
            : page.Cells.Where(IsMatchingWhereCondition).ToArray();

    private static string[] ParseSelectColumnNames(string selectPart) =>
        selectPart
            .Replace("select", "", StringComparison.OrdinalIgnoreCase)
            .Trim()
            .Split(',')
            .Select(c => c.Trim())
            .ToArray();

    private static (string tableName, string? whereColumn, string? whereValue) ParseFromClause(string fromPart)
    {
        if (!fromPart.Contains("where", StringComparison.OrdinalIgnoreCase))
            return (fromPart.Trim(), null, null);

        var parts = fromPart.Split("where", StringSplitOptions.RemoveEmptyEntries);
        var tableName = parts[0].Trim();

        var whereParts = parts[1].Trim().Split('=');
        if (whereParts.Length != 2)
            throw new ArgumentException("Invalid WHERE clause format");

        return (
            tableName,
            whereParts[0].Trim(),
            whereParts[1].Trim().Trim('\'')
        );
    }

    private bool IsMatchingWhereCondition(Cell cell)
    {
        if (WhereColumnIndex is null)
            return true;

        var columnValue = cell.Record.Columns.First(x => x.Index == WhereColumnIndex).Value?.ToString();

        return string.Equals(columnValue, WhereValue, StringComparison.CurrentCultureIgnoreCase);
    }
}