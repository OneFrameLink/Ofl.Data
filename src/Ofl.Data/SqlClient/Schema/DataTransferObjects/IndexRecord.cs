namespace Ofl.Data.SqlClient.Schema.DataTransferObjects
{
    internal class IndexRecord
    {
// ReSharper disable InconsistentNaming
#pragma warning disable IDE1006 // Naming Styles
        public int index_id { get; set; }
        public string name { get; set; }
        public bool is_primary_key { get; set; }
        public int column_id { get; set; }
        public int key_ordinal { get; set; }
        public bool is_descending_key { get; set; }
        public bool is_included_column { get; set; }
#pragma warning restore IDE1006 // Naming Styles
// ReSharper restore InconsistentNaming
    }
}
