using System;

namespace Ofl.Data.SqlClient
{
    /// <summary>Thrown when an invalid conversion takes place.</summary>
    public class SqlBulkCopyInvalidConversionException : Exception
    {
        /// <summary>Creates a new instance of the <see cref="SqlBulkCopyInvalidConversionException"/>
        /// class.</summary>
        /// <param name="message">The message to set the
        /// <see cref="Exception.Message"/> property to.</param>
        /// <param name="innerException">The <see cref="Exception"/>
        /// to set the <see cref="Exception.InnerException"/>
        /// property to.</param>
        /// <param name="item">The object that failed conversion.</param>
        /// <param name="ordinal">The ordinal in the sequence that
        /// failed, this value is zero-based.</param>
        /// <param name="sourceType">The type of the value in the source that
        /// failed conversion to the <paramref name="destinationType"/>.</param>
        /// <param name="destinationType">The type of the value in the destination
        /// that failed conversion from the <paramref name="sourceType"/>.</param>
        internal SqlBulkCopyInvalidConversionException(string message, Exception innerException,
            object item, int ordinal, string sourceType, string destinationType) : base(message, innerException)
        {
            // Validate the parameters.
            if (item == null) throw new ArgumentNullException(nameof(item));
            if (ordinal < 0) throw new ArgumentOutOfRangeException(nameof(ordinal), ordinal, "The ordinal parameter must be a non-negative number.");
            if (string.IsNullOrWhiteSpace(sourceType)) throw new ArgumentNullException(nameof(sourceType));
            if (string.IsNullOrWhiteSpace(destinationType)) throw new ArgumentNullException(nameof(destinationType));

            // Set the values.
            Item = item;
            Ordinal = ordinal;
            SourceType = sourceType;
            DestinationType = destinationType;
        }

        /// <summary>Gets the number of the item in the sequence that
        /// the conversion failed on.</summary>
        /// <remarks>This is zero-based, not one based, to make it easier to
        /// debug when looking through materialized sets.</remarks>
        /// <value>The number of the item in the sequence that
        /// the conversion failed on.</value>
        public int Ordinal { get; private set; }

        /// <summary>Gets the object that contains the property that
        /// failed the conversion.</summary>
        /// <value>The object that contains the property that
        /// failed the conversion.</value>
        public object Item { get; private set; }

        /// <summary>Gets the type of the data from the source.</summary>
        /// <value>The type of the data from the source.</value>
        public string SourceType { get; private set; }

        /// <summary>Gets the type of the data of the destination.</summary>
        /// <value>The type of the data on the destination.</value>
        public string DestinationType { get; private set; }
    }
}
