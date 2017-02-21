using System;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Ofl.Data.SqlClient.Contracts;

namespace Ofl.Data.SqlClient
{
    public static class SqlConnectionFactoryExtensions
    {
        public static async Task<SqlConnection> CreateOpenedConnectionAsync(
            this ISqlConnectionFactory factory, string name, CancellationToken cancellationToken)
        {
            // Validate the parameters.
            if (factory == null) throw new ArgumentNullException(nameof(factory));
            if (string.IsNullOrEmpty(name)) throw new ArgumentNullException(nameof(name));

            // The connection.
            SqlConnection connection = await factory.CreateConnectionAsync(name, cancellationToken).
                ConfigureAwait(false);

            // Open the connection.
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

            // Return the connection.
            return connection;
        }
    }
}
