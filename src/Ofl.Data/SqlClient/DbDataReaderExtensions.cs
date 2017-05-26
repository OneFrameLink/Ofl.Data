using Ofl.Interactive.Async;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data.Common;
using System.Linq;
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
            return AsyncEnumerable.CreateEnumerable(() => AsyncEnumerable.CreateEnumerator<DbDataReader>(
                ct => reader.ReadAsync(ct), () => reader, () => { }));
        }

        public static IAsyncEnumerable<T> ToAsyncEnumerable<T>(this DbDataReader reader, 
            Func<DbDataReader, CancellationToken, Task<T>> selector)
        {
            // Validate parameters.
            if (reader == null) throw new ArgumentNullException(nameof(reader));
            if (selector == null) throw new ArgumentNullException(nameof(selector));

            // The current value.
            T current = default(T);

            // Return as an async enumerable.
            return AsyncEnumerable.CreateEnumerable(() => AsyncEnumerable.CreateEnumerator<T>(
                async ct => {
                    // Move to the next item.
                    bool moveNext = await reader.ReadAsync(ct).ConfigureAwait(false);

                    // If false, get out.
                    if (!moveNext) return moveNext;

                    current = await selector(reader, ct).ConfigureAwait(false);

                    // Return move next.
                    return moveNext;
                }, 
                
                () => current, () => {}));
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
