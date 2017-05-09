using Ofl.Core.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using Ofl.Core.Hashing;

namespace Ofl.Data.SqlClient
{
    internal class MergeComponentsKey : IEquatable<MergeComponentsKey>
    {
        #region Constructor

        public MergeComponentsKey(string connectionString, string table, IEnumerable<string> columns)
        {
            // Validate parameters.
            if (string.IsNullOrWhiteSpace(connectionString)) throw new ArgumentNullException(nameof(connectionString));
            if (string.IsNullOrWhiteSpace(table)) throw new ArgumentNullException(nameof(table));
            if (columns == null) throw new ArgumentNullException(nameof(columns));

            // Assign values.
            ConnectionString = connectionString;
            Table = table;
            Columns = columns.OrderBy(c => c).ToReadOnlyCollection();
        }

        #endregion

        #region Instance, read-only state.

        public string ConnectionString { get; }
        public string Table { get; }
        public IReadOnlyCollection<string> Columns { get; }

        #endregion

        #region Implementation of IEquatable<MergeComponentsKey>

        protected virtual IEnumerable<int> GetStructuralHashCodes()
        {
            // Return empty for now.
            return Enumerable.Empty<int>();
        }

        /// <summary>
        /// Indicates whether the current object is equal to another object of the same type.
        /// </summary>
        /// <returns>
        /// true if the current object is equal to the <paramref name="other"/> parameter; otherwise, false.
        /// </returns>
        /// <param name="other">An object to compare with this object.</param>
        public bool Equals(MergeComponentsKey other)
        {
            // If null, not equal.
            if (other == null) return false;

            // Compare the rest.
            return
                ConnectionString == other.ConnectionString &&
                Table == other.Table &&
                Columns.SequenceEqual(other.Columns);
        }

        /// <summary>
        /// Determines whether the specified object is equal to the current object.
        /// </summary>
        /// <returns>
        /// true if the specified object  is equal to the current object; otherwise, false.
        /// </returns>
        /// <param name="obj">The object to compare with the current object. </param>
        public override bool Equals(object obj)
        {
            return Equals(obj as MergeComponentsKey);
        }

        #region Overrides of Object

        /// <summary>
        /// Serves as a hash function for a particular type. 
        /// </summary>
        /// <returns>
        /// A hash code for the current object.
        /// </returns>
        public override int GetHashCode()
        {
            // Create the hashcode lists.
            var hashCodes = new List<int> { ConnectionString.GetHashCode(), Table.GetHashCode() };

            // Add the columns.
            hashCodes.AddRange(Columns.Select(c => c.GetHashCode()));

            // Add anything else.
            hashCodes.AddRange(GetStructuralHashCodes());

            // Return the hash code.
            return hashCodes.Compute32BitFnvCompositeHashCode();
        }

        #endregion

        #endregion
    }
}
