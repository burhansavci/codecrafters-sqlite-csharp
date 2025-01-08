using codecrafters_sqlite.Sqlite.Pages;
using codecrafters_sqlite.Sqlite.Schemas;

namespace codecrafters_sqlite.Sqlite.Queries;

public record SqlQuery
{
    private ColumnInfo[]? _selectedColumns;

    public SqlQuery(string sql)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sql);

        var parts = sql.Split(" ", StringSplitOptions.RemoveEmptyEntries);

        if (!parts[0].Equals("select", StringComparison.InvariantCultureIgnoreCase))
            throw new ArgumentException("Only SELECT queries are supported", nameof(sql));

        var fromIndex = Array.FindIndex(parts, p => p.Equals("from", StringComparison.InvariantCultureIgnoreCase));
        if (fromIndex == -1)
            throw new ArgumentException("FROM clause is required", nameof(sql));

        // Parse SELECT columns
        var selectPart = string.Join(" ", parts.Take(fromIndex)).Replace("select", "", StringComparison.InvariantCultureIgnoreCase).Trim();
        SelectColumnNames = selectPart.Split(',', StringSplitOptions.RemoveEmptyEntries).Select(c => c.Trim()).ToArray();

        // Get table name and ensure it's not followed by other clauses
        if (fromIndex + 1 >= parts.Length)
            throw new ArgumentException("Table name is required after FROM", nameof(sql));

        TableName = parts[fromIndex + 1];

        // Parse WHERE clause if it exists
        var whereIndex = Array.FindIndex(parts, p => p.Equals("where", StringComparison.InvariantCultureIgnoreCase));
        if (whereIndex != -1)
        {
            if (whereIndex + 1 >= parts.Length)
                throw new ArgumentException("Condition is required after WHERE", nameof(sql));

            var whereClause = string.Join(" ", parts.Skip(whereIndex + 1));
            Where = new WhereClause(whereClause);
        }
    }

    public string TableName { get; }
    public WhereClause? Where { get; }
    public string[] SelectColumnNames { get; }
    public IReadOnlyList<ColumnInfo> SelectedColumns => _selectedColumns ?? throw new InvalidOperationException("Table schema not yet applied");

    public void ApplyTableSchema(SchemaEntry tableEntry)
    {
        ArgumentNullException.ThrowIfNull(tableEntry);
        ArgumentNullException.ThrowIfNull(tableEntry.Columns);

        // Map column names to their schema information
        _selectedColumns = new ColumnInfo[SelectColumnNames.Length];
        for (int i = 0; i < SelectColumnNames.Length; i++)
        {
            var columnName = SelectColumnNames[i];
            var column = tableEntry.Columns.FirstOrDefault(c => string.Equals(c.Name, columnName, StringComparison.InvariantCultureIgnoreCase));

            _selectedColumns[i] = column ?? throw new InvalidOperationException($"Column {columnName} not found in table schema");
        }

        // Apply WHERE clause schema if it exists
        if (Where == null)
            return;

        var whereColumn = tableEntry.Columns.FirstOrDefault(c => string.Equals(c.Name, Where.ColumnName, StringComparison.InvariantCultureIgnoreCase));

        if (whereColumn == null)
            throw new InvalidOperationException($"Column {Where.ColumnName} not found in table schema");

        Where.ApplyTableColumn(whereColumn);
    }

    public void ApplyIndexSchema(SchemaEntry? indexEntry)
    {
        if (Where == null || indexEntry == null)
            return;

        var column = indexEntry.Columns!.FirstOrDefault(c => string.Equals(c.Name, Where.ColumnName, StringComparison.InvariantCultureIgnoreCase));

        if (column != null)
            Where.ApplyIndexColumn(column);
    }

    public IEnumerable<Cell> GetResult(Page page)
    {
        ArgumentNullException.ThrowIfNull(page);
        return Where == null ? page.Cells : page.Cells.Where(cell => Where.Matches(cell));
    }
}