using System;
using System.ComponentModel.DataAnnotations.Schema;
using System.Globalization;
using Ofl.Data.SqlClient.Schema;

namespace Ofl.Data.SqlClient
{
    public static class TableExtensions
    {
        public static string GetSchemaQualifiedName(this Table table)
        {
            // Validate parameters.
            if (table == null) throw new ArgumentNullException(nameof(table));

            // Construct and return.
            return GetSchemaQualifiedName(table.Schema, table.Name);
        }

        public static string GetSchemaQualifiedName(this TableAttribute tableAttribute)
        {
            // Validate parameters.
            if (tableAttribute == null) throw new ArgumentNullException(nameof(tableAttribute));

            // Pass schema and name.
            return GetSchemaQualifiedName(tableAttribute.Schema, tableAttribute.Name);
        }

        public static string GetSchemaQualifiedName(string schema, string table)
        {
            // Validate parameters.  
            if (string.IsNullOrWhiteSpace(table)) throw new ArgumentNullException(nameof(table));
            
            // Format table.
            table = table.AsBracketedIdentifier();

            // If schema is null or whitespace, return table.
            if (string.IsNullOrWhiteSpace(schema)) return table;

            // Combine and return.
            return string.Format(CultureInfo.InvariantCulture, "{0}.{1}", schema.AsBracketedIdentifier(), table);
        }
    }
}
