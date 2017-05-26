using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Nito.AsyncEx;
using Ofl.Data.SqlClient.Schema;
using Ofl.Linq;

namespace Ofl.Data.SqlClient
{
    public static partial class SqlConnectionExtensions
    {
        private static readonly IDictionary<MergeComponentsKey, MergeComponents> MergeComponents =
            new Dictionary<MergeComponentsKey, MergeComponents>();

        private static readonly AsyncLock MergeComponentsLock = new AsyncLock();

        private static readonly ConcurrentDictionary<Type, Action<object, IDictionary<string, object>>> ValueFactories =
            new ConcurrentDictionary<Type, Action<object, IDictionary<string, object>>>();


        public static Task<SqlCommand> CreateMergeCommandAsync<T>(this SqlConnection connection,
            SqlTransaction transaction, T values, bool cache, CancellationToken cancellationToken)
        {
            // Validate parameters.
            if (connection == null) throw new ArgumentNullException(nameof(connection));
            if (values == null) throw new ArgumentNullException(nameof(values));

            // The type of T.
            Type type = typeof(T);

            // Get the table extension.
            var tableAttribute = type.GetTypeInfo().GetCustomAttribute<TableAttribute>(true);

            // Create the table name.
            string table = tableAttribute == null ? type.Name.AsBracketedIdentifier() : tableAttribute.GetSchemaQualifiedName();

            // Get properties that don't have not mapped.
            IReadOnlyCollection<PropertyInfo> properties = Reflection.TypeExtensions.
                GetPropertiesWithPublicInstanceGetters<T>().
                Where(pi => pi.GetCustomAttribute<NotMappedAttribute>() == null).
                ToReadOnlyCollection();

            // Call the overload.
            return connection.CreateMergeCommandAsync(transaction, table, values, properties, cache, cancellationToken);
        }

        public static Task<SqlCommand> CreateMergeCommandAsync<T>(this SqlConnection connection,
            SqlTransaction transaction, string table, T values, IReadOnlyCollection<PropertyInfo> properties,
            bool cache, CancellationToken cancellationToken)
        {
            // Validate parameters.
            if (connection == null) throw new ArgumentNullException(nameof(connection));
            if (string.IsNullOrWhiteSpace(table)) throw new ArgumentNullException(nameof(table));
            if (properties == null) throw new ArgumentNullException(nameof(properties));
            if (values == null) throw new ArgumentNullException(nameof(values));

            // Create the factory.
            Action<object, IDictionary<string, object>> populator = ValueFactories.GetOrAdd(typeof(T),
                t => CreateValueFactoryAction(values, properties));

            // The values.
            IDictionary<string, object> extractedValues = new Dictionary<string, object>();

            // Populate.
            populator(values, extractedValues);

            // Create the merge command and return.
            return connection.CreateMergeCommandAsync(transaction, table, extractedValues.ToReadOnlyDictionary(),
                cache, cancellationToken);
        }

        private static Action<object, IDictionary<string, object>> CreateValueFactoryAction<T>(T values, IReadOnlyCollection<PropertyInfo> properties)
        {
            // Validate parameters.
            Debug.Assert(values != null);
            Debug.Assert(properties != null);

            // The type of T.
            Type type = typeof(T);

            // Create the parameter.  Needs to be of type object.
            var valuesParameterExpression = Expression.Parameter(typeof(object), "values");

            // The map.
            var mapParameterExpression = Expression.Parameter(typeof(IDictionary<string, object>), "map");

            // The variable.
            var convertedValuesParameterExpression = Expression.Variable(type, "v");

            // The list of expressions.
            var bodyExpressions = new List<Expression> {
                Expression.Assign(convertedValuesParameterExpression,
                    Expression.Convert(valuesParameterExpression, type))
            };


            // The method info for the add method on the map.
            MethodInfo addMethodInfo = typeof(IDictionary<string, object>).GetMethod(
                "Add", new[] { typeof(string), typeof(object) });

            // Create the set of properties.
            ISet<PropertyInfo> propertiesSet = new HashSet<PropertyInfo>(properties);

            // Cycle through all public properties.
            bodyExpressions.AddRange(
                from pi in properties
                where propertiesSet.Contains(pi)
                let mapping = pi.GetColumnMapping()
                where mapping != null
                let formattedMapping = mapping.AsBracketedIdentifier()
                select Expression.Call(mapParameterExpression, addMethodInfo,
                    Expression.Constant(formattedMapping),
                    Expression.Convert(Expression.Property(convertedValuesParameterExpression, pi), typeof(object)))
            );

            // Create the body expression.
            var body = Expression.Block(new[] { convertedValuesParameterExpression }, bodyExpressions);

            // Create the lambda and return.
            return Expression.Lambda<Action<object, IDictionary<string, object>>>(body,
                valuesParameterExpression, mapParameterExpression).Compile();
        }

        public static async Task<SqlCommand> CreateMergeCommandAsync(this SqlConnection connection,
            SqlTransaction transaction, string table, IReadOnlyDictionary<string, object> values, bool cache, CancellationToken cancellationToken)
        {
            // Validate parameters.
            if (connection == null) throw new ArgumentNullException(nameof(connection));
            if (string.IsNullOrEmpty("table")) throw new ArgumentNullException(nameof(table));
            if (values == null) throw new ArgumentNullException(nameof(values));

            // Get the upsert components.
            MergeComponents mergeComponents = await connection.GetMergeComponentsAsync(transaction, table, values.Keys.ToReadOnlyCollection(), cache,
                cancellationToken).ConfigureAwait(false);

            // Create the command with the connection, cycle through the dictionary and add the parameters.
            var command = (transaction == null ? new SqlCommand(mergeComponents.Sql, connection) :
                new SqlCommand(mergeComponents.Sql, connection, transaction));

            // Cycle through the parameters, add.
            foreach (var pair in mergeComponents.ParameterMap)
            {
                // Do the lookup.
                values.TryGetValue(pair.Key, out object value);

                // Add the value.
                command.Parameters.AddWithValue(pair.Value, value ?? DBNull.Value);
            }

            // Return the command.
            return command;
        }

        private static async Task<MergeComponents> GetMergeComponentsAsync(this SqlConnection connection,
            SqlTransaction transaction, string table, IReadOnlyCollection<string> columns, bool cache, CancellationToken cancellationToken)
        {
            // Validate parameters.
            Debug.Assert(connection != null);
            Debug.Assert(!string.IsNullOrWhiteSpace(table));
            Debug.Assert(columns != null);

            // The table schema key.
            var key = connection.CreateMergeComponentsKey(table, columns);

            // Lock on the components lock.
            using (await MergeComponentsLock.LockAsync(cancellationToken).ConfigureAwait(false))
            {
                // If caching and we can get the value from the cache, then return it.
                if (cache && MergeComponents.TryGetValue(key, out MergeComponents mergeComponents)) return mergeComponents;

                // Create the merge components.
                mergeComponents = await connection.CreateMergeComponentsAsync(transaction,
                    table, columns, cache, cancellationToken).ConfigureAwait(false);

                // If caching, add back to the cache.
                if (cache) MergeComponents.Add(key, mergeComponents);

                // Return the merge components.
                return mergeComponents;
            }
        }

        private static MergeComponentsKey CreateMergeComponentsKey(this SqlConnection connection, string table,
            IEnumerable<string> columns)
        {
            // Validate parameters.
            if (connection == null) throw new ArgumentNullException(nameof(connection));
            if (string.IsNullOrWhiteSpace(table)) throw new ArgumentNullException(nameof(table));
            if (columns == null) throw new ArgumentNullException(nameof(columns));

            // Pass through.
            return new MergeComponentsKey(connection.ConnectionString, table, columns);
        }

        private static async Task<MergeComponents> CreateMergeComponentsAsync(this SqlConnection connection,
            SqlTransaction transaction, string table, IReadOnlyCollection<string> columns, bool cache, CancellationToken cancellationToken)
        {
            // Validate parameters.
            Debug.Assert(connection != null);
            Debug.Assert(columns != null);
            Debug.Assert(!string.IsNullOrWhiteSpace(table));

            // Get the table.
            Table tableSchema = await (transaction != null ? connection.GetTableSchemaAsync(transaction, table, cache, cancellationToken) :
                connection.GetTableSchemaAsync(table, cache, cancellationToken)).ConfigureAwait(false);

            // The table name.
            string tableName = tableSchema.GetSchemaQualifiedName();

            // The temp table name.
            string tempTable = "#__" + Guid.NewGuid().ToString("N");

            // The builder.
            var builder = new StringBuilder();

            // Create the temp table.
            builder.Append("select cast(null as varchar(10)) as __action, ");

            // Add the columns.
            foreach (string c in columns.Select(col => col.AsBracketedIdentifier()))
            {
                // Append the column name.
                builder.AppendFormat(CultureInfo.InvariantCulture, "t.{0}, ", c);
            }

            // Remove the last comma.
            builder.Length -= 2;

            // Close off.
            builder.AppendFormat(CultureInfo.InvariantCulture, " into {0} from {1} as t where 1 = 0; ",
                tempTable, tableName);

            // Start the merge command.
            builder.AppendFormat(CultureInfo.InvariantCulture, "merge {0} as t using (select ", tableName);

            // Get the places needed (since we're using base 10, log 10 of 10 is 1).
            var decimalPlaces = (int)Math.Ceiling(Math.Log10(columns.Count));

            // The format string.
            var formatString = new string('0', decimalPlaces);

            // Create the parameter mapping.  Use columns and an index.
            IReadOnlyDictionary<string, string> parameters = columns.
                Select((c, i) => new KeyValuePair<string, string>(
                    c.AsBracketedIdentifier(),
                    string.Format(CultureInfo.InvariantCulture, "@p{0:" + formatString + "}", i)
                )).ToReadOnlyDictionary();

            // The builder for the source parameters and the source alias.
            var sourceParameterBuilder = new StringBuilder();
            var sourceAliasBuilder = new StringBuilder();

            // Cycle through the parameters.
            foreach (var pair in parameters)
            {
                // Append.
                sourceParameterBuilder.AppendFormat(CultureInfo.InvariantCulture, "{0}, ", pair.Value);
                sourceAliasBuilder.AppendFormat(CultureInfo.InvariantCulture, "{0}, ", pair.Key);
            }

            // Trim the strings.
            sourceParameterBuilder.Length -= 2;
            sourceAliasBuilder.Length -= 2;

            // Append to the builder.
            builder.Append(sourceParameterBuilder).Append(") as s (").Append(sourceAliasBuilder).Append(") on (");

            // The primary key columns.
            ISet<string> primaryKeyColumns = new HashSet<string>(
                tableSchema.Indices.Values.Single(i => i.PrimaryKey).Columns.
                    Select(c => c.Column.Name.AsBracketedIdentifier()));

            // Now the primary keys.
            foreach (string pk in primaryKeyColumns)
            {
                // Append to the builder.
                builder.AppendFormat(CultureInfo.InvariantCulture, "t.{0} = s.{0} and ", pk);
            }

            // Trim off the last and.
            builder.Length -= 5;

            // Continue with the merge statement.
            builder.Append(") when matched then update set ");

            // Get the table schema columns as bracketed identifiers.
            IReadOnlyDictionary<string, Column> tableSchemaColumns =
                tableSchema.Columns.ToDictionary(p => p.Key.AsBracketedIdentifier(),
                    p => p.Value).ToReadOnlyDictionary();

            // Generate update statement.  Cycle through all of the columns where the primary key doesn't exist
            // and it's not computed.
            foreach (string col in columns)
            {
                // Get the column.
                Column c = tableSchemaColumns[col];

                // If this is computed, skip.
                if (c.Computed) continue;

                // The bracketed identifier name.
                string formattedName = c.Name.AsBracketedIdentifier();

                // If the column is a primary key, continue.
                if (primaryKeyColumns.Contains(formattedName)) continue;

                // Set the values.
                builder.AppendFormat(CultureInfo.InvariantCulture, "{0} = s.{0}, ", formattedName);
            }

            // Trim the end.
            builder.Length -= 2;

            // Insert.
            builder.Append(" when not matched then insert (");

            // The values builder.
            var valuesBuilder = new StringBuilder();

            // Insert everything but computed columns.
            foreach (string col in columns)
            {
                // Get the column.
                Column c = tableSchemaColumns[col];

                // If computed, skip.
                if (c.Computed) continue;

                // The bracketed identifier name.
                string formattedName = c.Name.AsBracketedIdentifier();

                // Add the column name.
                builder.AppendFormat(CultureInfo.InvariantCulture, "{0}, ", formattedName);

                // Add the value.
                valuesBuilder.AppendFormat(CultureInfo.InvariantCulture, "s.{0}, ", formattedName);
            }

            // Remove last comma.
            builder.Length -= 2;
            valuesBuilder.Length -= 2;

            // Close off values, add source.
            builder.AppendFormat(CultureInfo.InvariantCulture, ") values ({0}) output $action, ", valuesBuilder);

            // Output each field into the temp table.
            foreach (string c in columns.Select(col => col.AsBracketedIdentifier()))
            {
                // Append the column name.
                builder.AppendFormat(CultureInfo.InvariantCulture, "inserted.{0}, ", c);
            }

            // Remove the last two characters.
            builder.Length -= 2;

            // Append temp table, select, drop.
            builder.AppendFormat(CultureInfo.InvariantCulture, " into {0}; select * from {0}; drop table {0};", tempTable);

            // Return the components.
            return new MergeComponents(builder.ToString(), parameters);
        }
    }
}
