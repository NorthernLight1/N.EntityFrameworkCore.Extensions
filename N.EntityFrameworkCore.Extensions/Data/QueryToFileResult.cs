﻿namespace N.EntityFrameworkCore.Extensions;

public class QueryToFileResult
{
    public long BytesWritten { get; set; }
    public int DataRowCount { get; internal set; }
    public int TotalRowCount { get; internal set; }
}