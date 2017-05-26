using Ofl.Linq;
using System;
using System.Collections.Generic;

namespace Ofl.Data.SqlClient.Schema
{
    public class Index
    {
        #region Constructor

        public Index(int id, string name, bool primaryKey, IEnumerable<IndexColumn> columns)
        {
            // Validate parameters.
            if (string.IsNullOrWhiteSpace(name)) throw new ArgumentNullException(nameof(name));
            if (columns == null) throw new ArgumentNullException(nameof(columns));

            // Assign values.
            Id = id;
            Name = name;
            PrimaryKey = primaryKey;
            Columns = columns.ToReadOnlyCollection();
        }

        #endregion

        #region Instance, read-only state.

        public int Id { get; private set; }
        public string Name { get; private set; }
        public bool PrimaryKey { get; private set; }
        public IReadOnlyCollection<IndexColumn> Columns { get; private set; }

        #endregion
    }
}
