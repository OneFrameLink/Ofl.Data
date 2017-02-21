namespace Ofl.Data.SqlClient.Schema.DataTransferObjects
{
    internal class ColumnRecord
    {
        // ReSharper disable InconsistentNaming
        public int column_id { get; set; }
        public string name { get; set; }
        public bool is_identity { get; set; }
        public bool is_nullable { get; set; }
        public bool is_computed { get; set; }
        // ReSharper restore InconsistentNaming
    }
}
