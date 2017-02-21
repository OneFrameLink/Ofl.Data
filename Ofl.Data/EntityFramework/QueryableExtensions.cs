using System;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
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
    }
}
