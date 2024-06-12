using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.Configuration;

namespace N.EntityFrameworkCore.Extensions.Test.Common;

public class Config
{
    public static string GetConnectionString(string name)
    {
            var builder = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);

            return builder.Build().GetConnectionString(name);
        }
}