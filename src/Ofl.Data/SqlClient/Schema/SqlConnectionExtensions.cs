using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using Nito.AsyncEx;
using Ofl.Data.SqlClient.Schema.DataTransferObjects;
using Ofl.Linq;

namespace Ofl.Data.SqlClient.Schema
{
    public static class SqlConnectionExtensions
    {
        private static readonly IDictionary<TableSchemaKey, Table> Tables = new Dictionary<TableSchemaKey, Table>();

        private static readonly AsyncLock TablesLock = new AsyncLock();

        public static Task<Table> GetTableSchemaAsync(this SqlConnection connection,
            SqlTransaction transaction, string table, bool cache, CancellationToken cancellationToken)
        {
            // Validate parameters.
            if (connection == null) throw new ArgumentNullException(nameof(connection));
            if (transaction == null) throw new ArgumentNullException(nameof(transaction));
            if (string.IsNullOrWhiteSpace(table)) throw new ArgumentNullException(nameof(table));

            // Call the implementation.
            return connection.GetTableSchemaImplementationAsync(transaction, table, cache, cancellationToken);            
        }

        public static Task<Table> GetTableSchemaAsync(this SqlConnection connection, string table,
            bool cache, CancellationToken cancellationToken)
        {
            // Validate parameters.
            if (connection == null) throw new ArgumentNullException(nameof(connection));
            if (string.IsNullOrWhiteSpace(table)) throw new ArgumentNullException(nameof(table));

            // Call the implementation.
            return connection.GetTableSchemaImplementationAsync(null, table, cache, cancellationToken);
        }

        private static async Task<Table> GetTableSchemaImplementationAsync(this SqlConnection connection, 
            SqlTransaction transaction, string table, bool cache, CancellationToken cancellationToken)
        {
            // Validate parameters.
            Debug.Assert(connection != null);
            Debug.Assert(!string.IsNullOrWhiteSpace(table));

            // Create the key.
            var key = new TableSchemaKey {
                ConnectionString = connection.ConnectionString,
                Table = table
            };

            // If not caching, then get the schema table and return.
            if (!cache) return await connection.GetTableSchemaAsync(transaction, key, cancellationToken).ConfigureAwait(false);

            // Lock, get or add.
            using (await TablesLock.LockAsync(cancellationToken).ConfigureAwait(false))
            {
                // The table.
                if (!Tables.TryGetValue(key, out Table value))
                {
                    // Set value.
                    value = await connection.GetTableSchemaAsync(transaction, key, cancellationToken).
                        ConfigureAwait(false);

                    // Add.
                    Tables.Add(key, value);
                }

                // Return the value.
                return value;
            }
        }

        private static async Task<Table> GetTableSchemaAsync(this SqlConnection connection, 
            SqlTransaction transaction, TableSchemaKey key, CancellationToken cancellationToken)
        {
            // Validate parameters.
            Debug.Assert(connection != null);

            // The sql.
            const string sql = @"
select 
    s.name as schema_name, t.name as table_name, t.create_date, t.modify_date 
from 
    sys.tables as t 
        inner join sys.schemas as s on 
            s.schema_id = t.schema_id 
where 
    t.object_id = object_id(@table);
select
	c.column_id,
	c.name,
	c.is_identity,
	c.is_nullable,
	c.is_computed
from 
	sys.columns as c
where 
	c.object_id = object_id(@table);
select
	i.index_id,
	i.name,
	i.is_primary_key,
	ic.column_id,
	ic.key_ordinal,
	ic.is_descending_key,
	ic.is_included_column
from 
	sys.indexes as i
		inner join sys.index_columns as ic on
			ic.object_id = i.object_id and
			ic.index_id = i.index_id
where 
	i.object_id = object_id(@table);
";
            // Query multiple.
            using (SqlMapper.GridReader gridReader = await connection.QueryMultipleAsync(sql, 
                new { table = key.Table }, transaction: transaction, commandType: CommandType.Text).ConfigureAwait(false))
            {
                // Get the items.
                TableRecord tableRecord = (await gridReader.ReadAsync<TableRecord>().ConfigureAwait(false)).
                    Single();

                // Set the column records.
                IReadOnlyCollection<ColumnRecord> columnRecords = (await gridReader.ReadAsync<ColumnRecord>().
                    ConfigureAwait(false)).ToReadOnlyCollection();

                // Set the index records.
                IReadOnlyCollection<IndexRecord> indexRecords = (await gridReader.ReadAsync<IndexRecord>().
                    ConfigureAwait(false)).ToReadOnlyCollection();

                // Start creating everything, the columns first.
                IReadOnlyDictionary<int, Column> columns = columnRecords.
                    Select(c => new Column(c.column_id, c.name, c.is_identity, c.is_nullable, c.is_computed)).
                        ToReadOnlyDictionary(c => c.Id);

                // Get the indexes and index columns.
                IReadOnlyCollection<Index> indexes = indexRecords.
                    // Group the indexes first.
                    GroupBy(i => new { i.index_id, i.name, i.is_primary_key }).
                
                    // Select the index for each group.
                    Select(g => new Index(g.Key.index_id, g.Key.name, g.Key.is_primary_key,
                        // Get the individual columns.
                        g.Select(c => new IndexColumn(columns[c.column_id], c.key_ordinal, c.is_descending_key,
                            c.is_included_column)))).
                    // Materialize.
                    ToReadOnlyCollection();

                // Assemble the table and return.
                return new Table(tableRecord.schema_name, tableRecord.table_name, tableRecord.create_date,
                    tableRecord.modify_date, columns.Values.OrderBy(c => c.Id), indexes);
            }
        }
    }
}
