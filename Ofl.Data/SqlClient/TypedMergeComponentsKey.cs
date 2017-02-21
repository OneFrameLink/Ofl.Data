using System;
using System.Collections.Generic;

namespace Ofl.Data.SqlClient
{
    internal class TypedMergeComponentsKey : MergeComponentsKey, IEquatable<TypedMergeComponentsKey>
    {
        public TypedMergeComponentsKey(string connectionString, string table, IEnumerable<string> columns) : base(connectionString, table, columns)
        {
        }

        #region Implementation of IEquatable<TypedMergeComponentsKey>

        /// <summary>
        /// Indicates whether the current object is equal to another object of the same type.
        /// </summary>
        /// <returns>
        /// true if the current object is equal to the <paramref name="other"/> parameter; otherwise, false.
        /// </returns>
        /// <param name="other">An object to compare with this object.</param>
        public bool Equals(TypedMergeComponentsKey other)
        {
            throw new NotImplementedException();
        }

        #endregion
    }
}
