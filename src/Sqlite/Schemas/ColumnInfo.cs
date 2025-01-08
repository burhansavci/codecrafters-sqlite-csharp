namespace codecrafters_sqlite.Sqlite.Schemas;

public record ColumnInfo(string Name, string Type, int Index)
{
    private const string RowIdAliases = "integer primary key";
    private const string RowIdAliasException = "desc";

    // More info about RowIdAlias: https://www.sqlite.org/lang_createtable.html#rowid
    public bool IsRowIdAlias => Type.StartsWith(RowIdAliases, StringComparison.InvariantCultureIgnoreCase) &&
                                !Type.EndsWith(RowIdAliasException);
}