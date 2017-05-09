using Ofl.Core.Collections.Generic;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Ofl.Data.SqlClient
{
    public static class DbDataReaderExtensions
    {
        public static IAsyncEnumerable<DbDataReader> ToAsyncEnumerable(this DbDataReader reader)
        {
            // Validate parameters.
            if (reader == null) throw new ArgumentNullException(nameof(reader));

            // Return as an async enumerable.
            return AsyncEnumerableExtentions.Generate(ct => Task.FromResult(reader),
                (r, ct) => r.ReadAsync(ct), (r, ct) => Task.FromResult(r), (r, ct) => Task.FromResult(r));
        }

        public static IAsyncEnumerable<T> ToAsyncEnumerable<T>(this DbDataReader reader, 
            Func<DbDataReader, CancellationToken, Task<T>> selector)
        {
            // Validate parameters.
            if (reader == null) throw new ArgumentNullException(nameof(reader));
            if (selector == null) throw new ArgumentNullException(nameof(selector));

            // Return as an async enumerable.
            return AsyncEnumerableExtentions.Generate(ct => Task.FromResult(reader),
                (r, ct) => r.ReadAsync(ct), (r, ct) => Task.FromResult(r), selector);
        }

        public static Task<ReadOnlyCollection<T>> ToReadOnlyScalarCollectionAsync<T>(this DbDataReader reader, 
            CancellationToken cancellationToken)
        {
            // Validate the parameters.
            if (reader == null) throw new ArgumentNullException(nameof(reader));

            // Get the async enumerable.
            IAsyncEnumerable<T> enumerable = reader.ToAsyncEnumerable((r, ct) => r.GetFieldValueAsync<T>(0, ct));

            // Return read only collection.
            return enumerable.ToReadOnlyCollectionAsync(cancellationToken);
        }
    }
}
