using System;

namespace Ofl.Data.SqlClient.Schema
{
    public class IndexColumn
    {
        #region Constructor.

        public IndexColumn(Column column, int ordinal, bool descending, bool included)
        {
            // Validate parameters.
            if (column == null) throw new ArgumentNullException(nameof(column));

            // Assign values.
            Column = column;
            Ordinal = ordinal;
            Descending = descending;
            Included = included;
        }

        #endregion

        #region Instance, read-only state.

        public Column Column { get; private set; }
        public int Ordinal { get; private set; }
        public bool Descending { get; private set; }
        public bool Included { get; private set; }

        #endregion
    }
}
