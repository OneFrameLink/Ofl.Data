using System;

namespace Ofl.Data.SqlClient.Schema.DataTransferObjects
{
    internal class TableRecord
    {
// ReSharper disable InconsistentNaming
#pragma warning disable IDE1006 // Naming Styles
        public string schema_name { get; set; }
        public string table_name { get; set; }
        public DateTimeOffset create_date { get; set; }
        public DateTimeOffset modify_date { get; set; }
#pragma warning restore IDE1006 // Naming Styles
// ReSharper restore InconsistentNaming
    }
}
