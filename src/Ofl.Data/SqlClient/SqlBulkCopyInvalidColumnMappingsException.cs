using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Reflection;

namespace Ofl.Data.SqlClient
{
    /// <summary>Thrown when there is an invalid mapping to columns
    /// that do not exist in the database.</summary>
    public class SqlBulkCopyInvalidColumnMappingsException : Exception
    {
        /// <summary>Creates a new instance of the <see cref="SqlBulkCopyInvalidColumnMappingsException"/>.</summary>
        /// <param name="message">The message for the exception.</param>
        /// <param name="innerException">The <see cref="Exception"/>
        /// that caused this exception to be thrown.</param>
        /// <param name="columnToPropertyMap">The sequence of <see cref="KeyValuePair{TKey,TValue}"/>
        /// that maps the <see cref="PropertyInfo"/> to the column in the database
        /// that is invalid.</param>
        internal SqlBulkCopyInvalidColumnMappingsException(string message, Exception innerException, 
            IEnumerable<KeyValuePair<string, PropertyInfo>> columnToPropertyMap) :
            base(message, innerException)
        {
            // Validate parameters.
            if (columnToPropertyMap == null) throw new ArgumentNullException(nameof(columnToPropertyMap));

            // Assign values.
            InvalidColumnMappings = new ReadOnlyCollection<KeyValuePair<string, PropertyInfo>>(
                columnToPropertyMap.ToList());
        }

        /// <summary>Gets the sequence of <see cref="KeyValuePair{TKey,TValue}"/>
        /// that maps the <see cref="PropertyInfo"/> to the column in the database
        /// that is invalid.</summary>
        /// <value>The sequence of <see cref="KeyValuePair{TKey,TValue}"/>
        /// that maps the <see cref="PropertyInfo"/> to the column in the database
        /// that is invalid.</value>
        public IEnumerable<KeyValuePair<string, PropertyInfo>> InvalidColumnMappings { get; private set; }
    }
}
