using System;

namespace Ofl.Data.SqlClient.Schema.DataTransferObjects
{
    internal class TableRecord
    {
// ReSharper disable InconsistentNaming
        public string schema_name { get; set; }
        public string table_name { get; set; }
        public DateTimeOffset create_date { get; set; }
        public DateTimeOffset modify_date { get; set; }
// ReSharper restore InconsistentNaming
    }
}
