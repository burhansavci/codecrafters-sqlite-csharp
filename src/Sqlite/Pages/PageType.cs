namespace codecrafters_sqlite.Sqlite.Pages;

// B-tree Page Type: https://www.sqlite.org/fileformat.html#b_tree_pages
public enum PageType : byte
{
    /*
   The one-byte flag at offset 0 indicating the b-tree page type.

   A value of 2 (0x02) means the page is an interior index b-tree page.
   A value of 5 (0x05) means the page is an interior table b-tree page.
   A value of 10 (0x0a) means the page is a leaf index b-tree page.
   A value of 13 (0x0d) means the page is a leaf table b-tree page.

   Any other value for the b-tree page type is an error.
   */
    InteriorIndex = 0x02,
    InteriorTable = 0x05,
    LeafIndex = 0x0A,
    LeafTable = 0x0D
}