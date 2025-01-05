namespace codecrafters_sqlite.Sqlite.Schemas;

public record ColumnSchema
{
    private const string RowIdAliases = "integer primary key";
    private const string RowIdAliasException = "desc";

    public ColumnSchema(string[] parts, int index)
    {
        ArgumentNullException.ThrowIfNull(parts);

        if (parts.Length != 2)
            throw new ArgumentException("The parts must be consists of 2 part", nameof(parts));

        if (index < 0)
            throw new ArgumentException("The index must be postive");

        Name = parts[0];
        Type = parts.Length > 1 ? parts[1] : string.Empty;
        Index = index;
        IsRowIdAlias = Type.StartsWith(RowIdAliases, StringComparison.InvariantCultureIgnoreCase) && !Type.EndsWith(RowIdAliasException);
    }

    public string Name { get; private init; }
    public string Type { get; private init; }
    public int Index { get; private init; }
    public bool IsRowIdAlias { get; private init; }
}