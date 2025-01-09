using System.Text;
using codecrafters_sqlite.Sqlite.Pages;

namespace codecrafters_sqlite.Sqlite.Queries;

public record SqlQueryResult(Cell[] Cells, SqlQuery Query)
{
    private const char ColumnSeparator = '|';

    public override string ToString()
    {
        var sb = new StringBuilder();
        foreach (var cell in Cells)
        {
            if (cell.Record == null)
                continue;

            for (int i = 0; i < Query.SelectedColumns.Length; i++)
            {
                var column = Query.SelectedColumns[i];

                sb.Append(column.IsRowIdAlias ? cell.RowId : cell.Record.Columns[column.Index].Value);

                if (i < Query.SelectedColumns.Length - 1)
                    sb.Append(ColumnSeparator);
            }

            sb.AppendLine();
        }

        return sb.ToString();
    }
}