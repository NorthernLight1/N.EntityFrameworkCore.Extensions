using System;
using System.Collections.Generic;
using System.Text;

namespace N.EntityFrameworkCore.Extensions;

public class QueryToFileOptions
{
    public string ColumnDelimiter { get; set; }
    public int? CommandTimeout { get; set; }
    public bool IncludeHeaderRow { get; set; }
    public string RowDelimiter { get; set; }
    public string TextQualifer { get; set; }

    public QueryToFileOptions()
    {
            ColumnDelimiter = ",";
            IncludeHeaderRow = true;
            RowDelimiter = "\r\n";
            TextQualifer = "";
        }
}