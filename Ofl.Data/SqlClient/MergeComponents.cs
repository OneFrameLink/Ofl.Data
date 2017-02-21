using System;
using System.Collections.Generic;

namespace Ofl.Data.SqlClient
{
    internal struct MergeComponents
    {
        #region Constructor

        public MergeComponents(string sql, IReadOnlyDictionary<string, string> parameterMap)
        {
            // Validate parameters.
            if (string.IsNullOrWhiteSpace(sql)) throw new ArgumentNullException(nameof(sql));
            if (parameterMap == null) throw new ArgumentNullException(nameof(parameterMap));

            // Assign values.
            Sql = sql;
            ParameterMap = parameterMap;
        }

        #endregion

        #region Instance, read-only state.

        public readonly string Sql;

        public readonly IReadOnlyDictionary<string, string> ParameterMap;

        #endregion
    }
}
