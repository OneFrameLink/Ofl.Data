using System;

namespace Ofl.Data.SqlClient.Schema
{
    public class Column
    {
        #region Constructor.

        public Column(int id, string name, bool identity, bool nullable, bool computed)
        {
            // Validate parameters.
            if (string.IsNullOrWhiteSpace(name)) throw new ArgumentNullException(nameof(name));

            // Assign values.
            Id = id;
            Name = name;
            Identity = identity;
            Nullable = nullable;
            Computed = computed;
        }

        #endregion

        public int Id { get; private set; }

        public string Name { get; private set; }
        
        public bool Identity { get; private set; }

        public bool Nullable { get; private set; }

        public bool Computed { get; private set; }

    }
}
