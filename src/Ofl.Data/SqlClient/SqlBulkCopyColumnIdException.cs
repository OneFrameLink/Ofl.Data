using System;

namespace Ofl.Data.SqlClient
{
    /// <summary>The exception that indicates a general bulk copy exception
    /// where a column id is indicated.</summary>
    public sealed class SqlBulkCopyColumnIdException : Exception
    {
        /// <summary>Creates a new instance of the <see cref="SqlBulkCopyColumnIdException"/>.</summary>
        /// <param name="message">The message for the exception.</param>
        /// <param name="innerException">The <see cref="Exception"/> that
        /// was originally thrown.</param>
        /// <param name="property">The name of the property
        /// on the client side that had the issue.</param>
        /// <param name="column">The name of the column on the database side
        /// that had the issue.</param>
        internal SqlBulkCopyColumnIdException(string message, Exception innerException, string property, string column) :
            base(message, innerException)
        {
            // Validate parameters.
            if (string.IsNullOrWhiteSpace(property)) throw new ArgumentNullException(nameof(property));
            if (string.IsNullOrWhiteSpace(column)) throw new ArgumentNullException(nameof(column));

            // Assign values.
            Property = property;
            Column = column;
        }

        /// <summary>Gets the name of the property that had
        /// the issue on the client side.</summary>
        /// <value>The name of the property that had
        /// the issue on the client side.</value>
        public string Property { get; private set; }

        /// <summary>Gets the name of the column on the table
        /// on the server side that had the issue.</summary>
        /// <value>The name of the column on the table
        /// on the server side that had the issue.</value>
        public string Column { get; private set; }
    }
}
