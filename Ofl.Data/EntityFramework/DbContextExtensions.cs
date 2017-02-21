using System;
using Microsoft.EntityFrameworkCore;

namespace Ofl.Data.EntityFramework
{
    public static class DbContextExtensions
    {
        public static T WithNoTracking<T>(this T dbContext) where T : DbContext
        {
            // Validate parameters.
            if (dbContext == null) throw new ArgumentNullException(nameof(dbContext));

            // Disable tracking.
            dbContext.ChangeTracker.QueryTrackingBehavior = QueryTrackingBehavior.NoTracking;

            // Return the context.
            return dbContext;
        }
    }
}
