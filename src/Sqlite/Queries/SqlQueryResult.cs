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
            for (int i = 0; i < Query.Columns!.Length; i++)
            {
                var queryColumn = Query.Columns[i];
                var recordColumn = cell.Record!.Columns[queryColumn.Index];

                // Use cell rowId when PK is alias of rowid: https://www.sqlite.org/fileformat.html#representation_of_sql_tables
                if (queryColumn.IsRowIdAlias && recordColumn.Value is null)
                {
                    sb.Append(cell.RowId);
                }
                else
                {
                    sb.Append(recordColumn.Value);
                }

                if (i < Query.Columns.Length - 1)
                    sb.Append(ColumnSeparator);
            }

            sb.Append(Environment.NewLine);
        }

        return sb.ToString();
    }
}