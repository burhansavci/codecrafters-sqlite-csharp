using codecrafters_sqlite.Sqlite.Pages;

namespace codecrafters_sqlite.Sqlite.Schemas;

// Schema Table Format: https://www.sqlite.org/schematab.html
public record SchemaPage
{
    private readonly Page _page;

    public SchemaPage(Page page)
    {
        ArgumentNullException.ThrowIfNull(page);

        _page = page;
        Entries = _page.Cells.Select(cell => new SchemaEntry(cell)).ToArray();
    }

    public DbHeader DbHeader => _page.DbHeader!;
    public PageHeader Header => _page.Header;
    private SchemaEntry[] Entries { get; }
    public IEnumerable<SchemaEntry> Tables => Entries.Where(e => e.Type == "table" && e.Name != "sqlite_sequence");
    public IEnumerable<SchemaEntry> Indexes => Entries.Where(e => e.Type == "index");

    public SchemaEntry? GetTableEntry(string tableName) => Entries.FirstOrDefault(e => e.Type == "table" && e.TableName == tableName);

    public SchemaEntry? GetIndexEntry(string tableName) => Entries.FirstOrDefault(e => e.Type == "index" && e.TableName == tableName);
}