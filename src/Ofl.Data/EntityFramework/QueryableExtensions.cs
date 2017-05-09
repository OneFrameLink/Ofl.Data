using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Ofl.Core.Collections.Generic;
using Ofl.Core.Linq;

namespace Ofl.Data.EntityFramework
{
    public static class QueryableExtensions
    {
        public static async Task<ReadOnlyCollection<T>> ToReadOnlyCollectionAsync<T>(this IQueryable<T> queryable,
            CancellationToken cancellationToken)
        {
            // Validate parameters.
            if (queryable == null) throw new ArgumentNullException(nameof(queryable));

            // Call a list, wrap in a read only collection.
            return (await queryable.ToListAsync(cancellationToken).ConfigureAwait(false)).
                WrapInReadOnlyCollection();
        }

        public static async Task<ReadOnlyDictionary<TKey, T>> ToReadOnlyDictionaryAsync<T, TKey>(this IQueryable<T> queryable,
            Func<T, TKey> keySelector, CancellationToken cancellationToken)
        {
            // Validate parameters.
            if (queryable == null) throw new ArgumentNullException(nameof(queryable));
            if (keySelector == null) throw new ArgumentNullException(nameof(keySelector));

            // Create a dictionary and wrap.
            Dictionary<TKey, T> dictionary = await queryable.ToDictionaryAsync(keySelector, cancellationToken).
                ConfigureAwait(false);

            // Wrap.
            return dictionary.WrapInReadOnlyDictionary();
        }

        public static async Task<ReadOnlyDictionary<TKey, TValue>> ToReadOnlyDictionaryAsync<T, TKey, TValue>(this IQueryable<T> queryable,
            Func<T, TKey> keySelector, Func<T, TValue> elementSelector, CancellationToken cancellationToken)
        {
            // Validate parameters.
            if (queryable == null) throw new ArgumentNullException(nameof(queryable));
            if (keySelector == null) throw new ArgumentNullException(nameof(keySelector));
            if (elementSelector == null) throw new ArgumentNullException(nameof(elementSelector));

            // Create a dictionary and wrap.
            Dictionary<TKey, TValue> dictionary = await queryable.ToDictionaryAsync(keySelector, elementSelector, cancellationToken).
                ConfigureAwait(false);

            // Wrap.
            return dictionary.WrapInReadOnlyDictionary();
        }

        public static async Task<ReadOnlyDictionary<TKey, T>> ToReadOnlyDictionaryAsync<T, TKey>(this IQueryable<T> queryable,
            Func<T, TKey> keySelector, IEqualityComparer<TKey> comparer,
            CancellationToken cancellationToken)
        {
            // Validate parameters.
            if (queryable == null) throw new ArgumentNullException(nameof(queryable));
            if (keySelector == null) throw new ArgumentNullException(nameof(keySelector));
            if (comparer == null) throw new ArgumentNullException(nameof(comparer));

            // Create a dictionary and wrap.
            Dictionary<TKey, T> dictionary = await queryable.ToDictionaryAsync(keySelector, comparer, cancellationToken).
                ConfigureAwait(false);

            // Wrap.
            return dictionary.WrapInReadOnlyDictionary();
        }

        public static async Task<ReadOnlyDictionary<TKey, TValue>> ToReadOnlyDictionaryAsync<T, TKey, TValue>(this IQueryable<T> queryable,
            Func<T, TKey> keySelector, Func<T, TValue> elementSelector, IEqualityComparer<TKey> comparer,
            CancellationToken cancellationToken)
        {
            // Validate parameters.
            if (queryable == null) throw new ArgumentNullException(nameof(queryable));
            if (keySelector == null) throw new ArgumentNullException(nameof(keySelector));
            if (elementSelector == null) throw new ArgumentNullException(nameof(elementSelector));
            if (comparer == null) throw new ArgumentNullException(nameof(comparer));

            // Create a dictionary and wrap.
            Dictionary<TKey, TValue> dictionary = await queryable.ToDictionaryAsync(keySelector, elementSelector, comparer, cancellationToken).
                ConfigureAwait(false);

            // Wrap.
            return dictionary.WrapInReadOnlyDictionary();
        }
    }
}
