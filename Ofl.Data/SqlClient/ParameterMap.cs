using System;
using System.Data.SqlClient;
using Ofl.Data.SqlClient.Schema;

namespace Ofl.Data.SqlClient
{
    public class ParameterMap
    {
        #region Constructor

        public ParameterMap(SqlParameter parameter, Column column)
        {
            // Validate parameters.
            if (parameter == null) throw new ArgumentNullException(nameof(parameter));
            if (column == null) throw new ArgumentNullException(nameof(column));

            // Assign values.
            Parameter = parameter;
            Column = column;
        }

        #endregion

        #region Instance, read-only state.

        public SqlParameter Parameter { get; private set; }
        public Column Column { get; private set; }

        #endregion
    }
}
