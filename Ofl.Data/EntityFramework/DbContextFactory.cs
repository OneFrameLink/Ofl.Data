using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Ofl.Data.EntityFramework.Contracts;

namespace Ofl.Data.EntityFramework
{
    public class DbContextFactory : IDbContextFactory
    {
        #region Constructor

        public DbContextFactory(ILoggerFactory loggerFactory)
        {
            // Validate parameters.
            if (loggerFactory == null) throw new ArgumentNullException(nameof(loggerFactory));

            // Assign values.
            _loggerFactory = loggerFactory;
            _logger = _loggerFactory.CreateLogger<DbContextFactory>();
        }

        #endregion

        #region Instance, read-only state.

        private readonly ILogger _logger;

        private readonly ILoggerFactory _loggerFactory;

        #endregion

        #region Implementation of IDbContextFactory

        public async Task<T> CreateDbContextAsync<T>(
            Func<DbContextOptionsBuilder<T>, CancellationToken, Task> configuration,
            CancellationToken cancellationToken) where T : DbContext
        {
            // Validate parameters.
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));

            // Create the builder.
            var builder = new DbContextOptionsBuilder<T>().UseLoggerFactory(_loggerFactory);

            // Configure.
            await configuration(builder, cancellationToken).ConfigureAwait(false);

            // TODO: Determine whether or not this would benefit from dynamic code compilation (lambdas).
            var t = (T) Activator.CreateInstance(typeof(T), builder.Options);

            // Return t.
            return t;
        }

        #endregion
    }
}
