using Ofl.Configuration;
using System;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

namespace Ofl.Data.SqlClient
{
    //////////////////////////////////////////////////
    ///
    /// <author>Nicholas Paldino</author>
    /// <created>2014-06-29</created>
    /// <summary>Used to create instances of <see cref="SqlConnection"/>.</summary>
    ///
    //////////////////////////////////////////////////
    public class SqlConnectionFactory : ISqlConnectionFactory
    {
        #region Constructor

        public SqlConnectionFactory(IConnectionStringManager connectionStringManager)
        {
            // Validate parameters.
            _connectionStringManager = connectionStringManager ?? throw new ArgumentNullException(nameof(connectionStringManager));
        }

        #endregion

        #region Instance, read-only state.

        private readonly IConnectionStringManager _connectionStringManager;

        #endregion

        #region ISqlConnectionFactory

        public async Task<SqlConnection> CreateConnectionAsync(string name, CancellationToken cancellationToken)
        {
            // Validate parameters.
            if (string.IsNullOrEmpty(name)) throw new ArgumentNullException(nameof(name));

            // Get the connection string, create the connection and return.
            return new SqlConnection(await _connectionStringManager.GetConnectionStringAsync(name, cancellationToken).
                ConfigureAwait(false));
        }

        #endregion
    }
}
