namespace codecrafters_sqlite.Sqlite.Pages;

public record Cell(uint? LeftChildPageNumber, long? Size, long? RowId, Record? Record, uint? FirstOverflowPageNumber);