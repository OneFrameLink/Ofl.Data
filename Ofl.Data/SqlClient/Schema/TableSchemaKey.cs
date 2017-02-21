using System;

namespace Ofl.Data.SqlClient.Schema
{
    internal struct TableSchemaKey
    {
        #region Constructor.

        public TableSchemaKey(string connectionString, string table)
        {
            // Validate parameters.
            if (string.IsNullOrWhiteSpace(connectionString)) throw new ArgumentNullException(nameof(connectionString));
            if (string.IsNullOrWhiteSpace(table)) throw new ArgumentNullException(nameof(table));

            // Assign values.
            ConnectionString = connectionString;
            Table = table;
        }

        #endregion

        #region Instance, read-only state.

        public string ConnectionString;
        public string Table;

        #endregion
    }
}
