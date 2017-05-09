using System;
using System.Collections.Generic;
using Ofl.Core.Linq;

namespace Ofl.Data.SqlClient.Schema
{
    public class Table
    {
        #region Constructor

        public Table(string schema, string name, DateTimeOffset created,
            DateTimeOffset lastModified, IEnumerable<Column> columns, IEnumerable<Index> indices)
        {
            // Validate parameters.
            if (string.IsNullOrWhiteSpace(schema)) throw new ArgumentNullException(nameof(schema));
            if (string.IsNullOrWhiteSpace(name)) throw new ArgumentNullException(nameof(name));
            if (columns == null) throw new ArgumentNullException(nameof(columns));

            // Assign values.
            Schema = schema;
            Name = name;
            Created = created;
            LastModified = lastModified;
            Columns = columns.ToReadOnlyDictionary(c => c.Name);
            Indices = indices.ToReadOnlyDictionary(i => i.Name);
        }

        #endregion

        #region Instance, read-only state.

        public string Schema { get; private set; }

        public string Name { get; private set; }

        public DateTimeOffset Created { get; private set; }

        public DateTimeOffset LastModified { get; private set; }

        public IReadOnlyDictionary<string, Column> Columns { get; private set; }

        public IReadOnlyDictionary<string, Index> Indices { get; private set; }

        #endregion
    }
}
