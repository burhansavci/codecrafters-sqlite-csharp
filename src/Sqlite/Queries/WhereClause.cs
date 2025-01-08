using codecrafters_sqlite.Sqlite.Pages;
using codecrafters_sqlite.Sqlite.Schemas;

namespace codecrafters_sqlite.Sqlite.Queries;

public record WhereClause
{
    private ColumnInfo? _tableColumn;

    public WhereClause(string clause)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clause);

        var parts = clause.Split('=');
        if (parts.Length != 2)
            throw new ArgumentException("Invalid WHERE clause format", nameof(clause));

        ColumnName = parts[0].Trim();
        Value = parts[1].Trim().Trim('\'');
    }

    public string ColumnName { get; }
    public string Value { get; }
    public ColumnInfo? IndexColumn { get; private set; }

    public void ApplyTableColumn(ColumnInfo column)
    {
        ArgumentNullException.ThrowIfNull(column);
        _tableColumn = column;
    }

    public void ApplyIndexColumn(ColumnInfo column)
    {
        ArgumentNullException.ThrowIfNull(column);
        IndexColumn = column;
    }

    public bool Matches(Cell cell)
    {
        ArgumentNullException.ThrowIfNull(cell);

        if (cell.Record == null)
            return false;

        if (IndexColumn != null)
        {
            var column = cell.Record.Columns[0];
            if (column.Value == null)
                return false;

            if (_tableColumn!.IsRowIdAlias)
                return cell.RowId == long.Parse(Value);

            return string.Equals(column.Value.ToString(), Value, StringComparison.InvariantCultureIgnoreCase);
        }

        if (_tableColumn!.IsRowIdAlias)
            return cell.RowId == long.Parse(Value);

        var columnValue = cell.Record.Columns[_tableColumn.Index].Value;
        if (columnValue == null)
            return false;

        return string.Equals(columnValue.ToString(), Value, StringComparison.InvariantCultureIgnoreCase);
    }
}