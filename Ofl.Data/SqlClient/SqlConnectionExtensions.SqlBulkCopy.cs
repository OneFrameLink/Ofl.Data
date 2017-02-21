using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Dapper;

namespace Ofl.Data.SqlClient
{
    public static partial class SqlConnectionExtensions
    {
        /// <summary>
        /// <para>As per:</para>
        /// <para>http://msdn.microsoft.com/en-us/library/system.data.sqlclient.sqlbulkcopy.bulkcopytimeout.aspx</para>
        /// </summary>
        private static readonly TimeSpan DefaultBulkCopyTimeout = TimeSpan.FromSeconds(30);

        /// <summary>
        /// <para>As per:</para>
        /// <para>http://msdn.microsoft.com/en-us/library/system.data.sqlclient.sqlbulkcopy.batchsize.aspx</para>
        /// </summary>
        private const int DefaultBulkCopyBatchSize = 0;

        /// <summary>Bulk copies a sequence of instances of <typeparamref name="T"/> to the database.</summary>
        /// <typeparam name="T">The type that is bulk copied to the database.</typeparam>
        /// <remarks>
        /// <para>Reflection is not used for getting the values from the <paramref name="items"/>
        /// sequence; reflection is used to perform the mapping, but the mapping itself is compiled
        /// code generated on-the-fly for performance.</para>
        /// <para>The name of the type is used as the table name (with "dbo" as the default schema), or
        /// if the <see cref="TableAttribute"/> is applied, then the <see cref="TableAttribute.Name"/>
        /// property is used.</para>
        /// <para>The names of the public properties are mapped to the names of the columns, or, if the
        /// property has the <see cref="ColumnAttribute"/> applied to it, then
        /// <see cref="ColumnAttribute.Name"/> is used.</para>
        /// <para>Uses the <see cref="DefaultBulkCopyTimeout"/> and the <see cref="DefaultBulkCopyBatchSize"/>.</para>
        /// </remarks>
        /// <param name="connection">The <see cref="SqlConnection"/> that is used to perform the bulk copy.</param>
        /// <param name="items">The sequence of instances of <typeparamref name="T"/>
        /// to bulk copy to the database.</param>
        /// <param name="cancellationToken">The <see cref="CancellationToken"/> used to
        /// cancel the operation, if necessary.</param>
        public static Task SqlBulkCopyAsync<T>(this SqlConnection connection, IEnumerable<T> items, CancellationToken cancellationToken)
        {
            // Call the overload.
            return connection.SqlBulkCopyAsync(items, BulkCopyDataReader<T>.DefaultDestinationTableName, cancellationToken);
        }

        /// <summary>Bulk copies a sequence of instances of <typeparamref name="T"/> to the database.</summary>
        /// <typeparam name="T">The type that is bulk copied to the database.</typeparam>
        /// <remarks>
        /// <para>Reflection is not used for getting the values from the <paramref name="items"/>
        /// sequence; reflection is used to perform the mapping, but the mapping itself is compiled
        /// code generated on-the-fly for performance.</para>
        /// <para>The name of the type is used as the table name (with "dbo" as the default schema), or
        /// if the <see cref="TableAttribute"/> is applied, then the <see cref="TableAttribute.Name"/>
        /// property is used.</para>
        /// <para>The names of the public properties are mapped to the names of the columns, or, if the
        /// property has the <see cref="ColumnAttribute"/> applied to it, then
        /// <see cref="ColumnAttribute.Name"/> is used.</para>
        /// <para>Uses the <see cref="DefaultBulkCopyTimeout"/> and the <see cref="DefaultBulkCopyBatchSize"/>.</para>
        /// </remarks>
        /// <param name="connection">The <see cref="SqlConnection"/> that is used to perform the bulk copy.</param>
        /// <param name="items">The sequence of instances of <typeparamref name="T"/>
        /// to bulk copy to the database.</param>
        /// <param name="table">The name of the table that the <paramref name="items"/>
        /// should be copied to.</param>
        /// <param name="cancellationToken">The <see cref="CancellationToken"/> used to
        /// cancel the operation, if necessary.</param>
        public static Task SqlBulkCopyAsync<T>(this SqlConnection connection, IEnumerable<T> items, string table,
            CancellationToken cancellationToken)
        {
            // Call the overload.
            return connection.SqlBulkCopyAsync(items, table, DefaultBulkCopyTimeout, DefaultBulkCopyBatchSize, SqlBulkCopyOptions.Default,
                cancellationToken);
        }

        /// <summary>Bulk copies a sequence of instances of <typeparamref name="T"/> to the database.</summary>
        /// <typeparam name="T">The type that is bulk copied to the database.</typeparam>
        /// <remarks>
        /// <para>Reflection is not used for getting the values from the <paramref name="items"/>
        /// sequence; reflection is used to perform the mapping, but the mapping itself is compiled
        /// code generated on-the-fly for performance.</para>
        /// <para>The name of the type is used as the table name (with "dbo" as the default schema), or
        /// if the <see cref="TableAttribute"/> is applied, then the <see cref="TableAttribute.Name"/>
        /// property is used.</para>
        /// <para>The names of the public properties are mapped to the names of the columns, or, if the
        /// property has the <see cref="ColumnAttribute"/> applied to it, then
        /// <see cref="ColumnAttribute.Name"/> is used.</para>
        /// </remarks>
        /// <param name="connection">The <see cref="SqlConnection"/> that is used to perform the bulk copy.</param>
        /// <param name="items">The sequence of instances of <typeparamref name="T"/>
        /// to bulk copy to the database.</param>
        /// <param name="commandTimeout">The timeout to assign to the operation.</param>
        /// <param name="cancellationToken">The <see cref="CancellationToken"/> used to
        /// cancel the operation, if necessary.</param>
        public static Task SqlBulkCopyAsync<T>(this SqlConnection connection, IEnumerable<T> items, TimeSpan commandTimeout,
            CancellationToken cancellationToken)
        {
            // Call the overload.
            return connection.SqlBulkCopyAsync(items, BulkCopyDataReader<T>.DefaultDestinationTableName, commandTimeout, DefaultBulkCopyBatchSize,
                SqlBulkCopyOptions.Default, cancellationToken);
        }

        /// <summary>Bulk copies a sequence of instances of <typeparamref name="T"/> to the database.</summary>
        /// <typeparam name="T">The type that is bulk copied to the database.</typeparam>
        /// <remarks>
        /// <para>Reflection is not used for getting the values from the <paramref name="items"/>
        /// sequence; reflection is used to perform the mapping, but the mapping itself is compiled
        /// code generated on-the-fly for performance.</para>
        /// <para>The name of the type is used as the table name (with "dbo" as the default schema), or
        /// if the <see cref="TableAttribute"/> is applied, then the <see cref="TableAttribute.Name"/>
        /// property is used.</para>
        /// <para>The names of the public properties are mapped to the names of the columns, or, if the
        /// property has the <see cref="ColumnAttribute"/> applied to it, then
        /// <see cref="ColumnAttribute.Name"/> is used.</para>
        /// </remarks>
        /// <param name="connection">The <see cref="SqlConnection"/> that is used to perform the bulk copy.</param>
        /// <param name="items">The sequence of instances of <typeparamref name="T"/>
        /// to bulk copy to the database.</param>
        /// <param name="commandTimeout">The timeout to assign to the operation.</param>
        /// <param name="sqlBulkCopyOptions">Values from the <see cref="SqlBulkCopyOptions"/>
        /// that set the options when bulk copying to the database.</param>
        /// <param name="batchSize">The size of the batch to send to SQL server.</param>
        /// <param name="cancellationToken">The <see cref="CancellationToken"/> used to
        /// cancel the operation, if necessary.</param>
        public static Task SqlBulkCopyAsync<T>(this SqlConnection connection, IEnumerable<T> items, TimeSpan commandTimeout, int batchSize,
            SqlBulkCopyOptions sqlBulkCopyOptions, CancellationToken cancellationToken)
        {
            // Call the overload.
            return connection.SqlBulkCopyAsync(items, BulkCopyDataReader<T>.DefaultDestinationTableName, commandTimeout, batchSize, sqlBulkCopyOptions,
                cancellationToken);
        }

        /// <summary>Bulk copies a sequence of instances of <typeparamref name="T"/> to the database.</summary>
        /// <typeparam name="T">The type that is bulk copied to the database.</typeparam>
        /// <remarks>
        /// <para>Reflection is not used for getting the values from the <paramref name="items"/>
        /// sequence; reflection is used to perform the mapping, but the mapping itself is compiled
        /// code generated on-the-fly for performance.</para>
        /// <para>The name of the type is used as the table name (with "dbo" as the default schema), or
        /// if the <see cref="TableAttribute"/> is applied, then the <see cref="TableAttribute.Name"/>
        /// property is used.</para>
        /// <para>The names of the public properties are mapped to the names of the columns, or, if the
        /// property has the <see cref="ColumnAttribute"/> applied to it, then
        /// <see cref="ColumnAttribute.Name"/> is used.</para>
        /// </remarks>
        /// <param name="connection">The <see cref="SqlConnection"/> that is used to perform the bulk copy.</param>
        /// <param name="items">The sequence of instances of <typeparamref name="T"/>
        /// to bulk copy to the database.</param>
        /// <param name="commandTimeout">The timeout to assign to the operation.</param>
        /// <param name="sqlBulkCopyOptions">Values from the <see cref="SqlBulkCopyOptions"/>
        /// that set the options when bulk copying to the database.</param>
        /// <param name="table">The name of the table that the <paramref name="items"/>
        /// should be copied to.</param>
        /// <param name="batchSize">The size of the batch to send to SQL server.</param>
        /// <param name="cancellationToken">The <see cref="CancellationToken"/> used to
        /// cancel the operation, if necessary.</param>
        public static async Task SqlBulkCopyAsync<T>(this SqlConnection connection, IEnumerable<T> items, string table, TimeSpan commandTimeout, int batchSize,
            SqlBulkCopyOptions sqlBulkCopyOptions, CancellationToken cancellationToken)
        {
            // Validate the parameters.
            if (connection == null) throw new ArgumentNullException(nameof(connection));
            if (items == null) throw new ArgumentNullException(nameof(items));
            if (string.IsNullOrWhiteSpace(table)) throw new ArgumentNullException(nameof(table));

            // Validate the batch size.
            if (batchSize < 0) throw new ArgumentOutOfRangeException(nameof(batchSize), batchSize, "The batchSize parameter must be a non-negative value.");

            // Get the command timeout in seconds.
            var commandTimeoutSeconds = (int)commandTimeout.TotalSeconds;

            // If 0 or less, throw an exception.
            if (commandTimeoutSeconds <= 0) throw new ArgumentOutOfRangeException(nameof(commandTimeout), commandTimeout,
                "The commandTimeout parameter must be a positive value.");

            // Create the sql bulk copy.
            using (var bc = new SqlBulkCopy(connection, sqlBulkCopyOptions, null))
            {
                // Create the reader.
                var reader = new BulkCopyDataReader<T>(items);

                // Initialize bulk copier.
                bc.BatchSize = batchSize;
                bc.BulkCopyTimeout = commandTimeoutSeconds;

                // Set properties from the reader.  Destination table name.
                bc.DestinationTableName = table;

                // Configure mappings, this is from the field name in the
                // destination to an index-based lookup in the source (the
                // enumerables).
                bc.ColumnMappings.Clear();
                foreach (KeyValuePair<int, BulkCopyDataReader<T>.Mapping> mapping in
                    BulkCopyDataReader<T>.OrdinalToMappingMap)
                {
                    bc.ColumnMappings.Add(mapping.Key, mapping.Value.Column);
                }

                // Wrap in a try/catch.
                try
                {
                    // Write the values.
                    await bc.WriteToServerAsync(reader, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    // The exception to throw.
                    Exception exceptionToThrow = null;

                    // Try to parse, if there's an exception, throw the original.
                    try
                    {
                        // Try and get the BulkException.
                        exceptionToThrow = BulkCopyDataReader<T>.GetBulkCopyColumnIdException(e, connection, table);

                        // Try and get the column mappings exception.
                        exceptionToThrow = exceptionToThrow ?? BulkCopyDataReader<T>.GetBulkCopyInvalidColumnMappingsException(e, bc);

                        // Try and get the invalid conversion exception.
                        exceptionToThrow = exceptionToThrow ?? BulkCopyDataReader<T>.GetBulkCopyInvalidConversionException(e, reader);
                    }
                    catch
                    {
                        // Intentionally do nothing. 
                    }

                    // Throw the exception if not null.
                    if (exceptionToThrow != null) throw exceptionToThrow;

                    // Throw the original exception.
                    throw;
                }
            }
        }

        #region Static read-only state for the reader class.

        private static readonly FieldInfo SortedColumnMappingsFieldInfo = typeof(SqlBulkCopy).GetField("_sortedColumnMappings",
            BindingFlags.Instance | BindingFlags.NonPublic);

        /// <summary>The <see cref="Regex"/> that is used to determine if the
        /// message in the exception has a "colid #" pattern.</summary>
        private static readonly Regex ColIdRegEx = new Regex(@"(?<toBeReplaced>colid[\s](?<columnId>[0-9]+))",
            RegexOptions.Singleline);

        /// <summary>Used to match the exception message on the
        /// <see cref="InvalidOperationException"/> that is thrown
        /// when a conversion cannot take place.</summary>
        private static readonly Regex InvalidConversionRegex = new Regex(
            @"^The given value of type (?<sourceType>.+?) from the data source cannot be converted to type (?<destinationType>.+?) of the specified target column.$",
            RegexOptions.Singleline);

        #endregion


        /// <summary>An internal class implementing <see cref="IDataReader"/>
        /// which reads the items from the sequence as-needed
        /// to the bulk import process.</summary>
        /// <typeparam name="T">The type of the items mapped to the table to
        /// be bulk copied to.</typeparam>
        private class BulkCopyDataReader<T> : DbDataReader
        {

            #region Mapping to get values from T

            internal class Mapping
            {
                internal Mapping(PropertyInfo property, string column, Func<T, object> propertyAccessor)
                {
                    // Validate parameters.
                    Debug.Assert(property != null);
                    Debug.Assert(!string.IsNullOrWhiteSpace(column));
                    Debug.Assert(propertyAccessor != null);

                    // Assign values.
                    Column = column;
                    Property = property;
                    PropertyAccessor = propertyAccessor;
                }

                internal string Column { get; }
                internal PropertyInfo Property { get; }
                internal Func<T, object> PropertyAccessor { get; }
            }

            #endregion

            #region Static read-only/const state.

            private const string DefaultSchema = "dbo";

            // ReSharper disable StaticMemberInGenericType

            internal static readonly string DefaultDestinationTableName;

            private static readonly int CachedFieldCount;

            // ReSharper restore StaticMemberInGenericType

            internal static readonly IDictionary<int, Mapping> OrdinalToMappingMap;

            private static readonly IDictionary<string, Mapping> ColumnToMappingMap;

            #endregion

            #region Static constructor and helpers for static constructor

            static BulkCopyDataReader()
            {
                // The type of T.
                Type type = typeof(T);

                // Get the table attribute.
                TableAttribute tableAttribute = type.GetTypeInfo().GetCustomAttributes(typeof(TableAttribute), true).
                    Cast<TableAttribute>().SingleOrDefault();

                // Use the name of the class initially, unless
                // there is a table attribute.
                string table = tableAttribute == null ? type.Name : tableAttribute.Name;

                // If there is no ".", assume default schema.
                if (!table.Contains(".")) table = DefaultSchema + "." + table;

                // Set the destination table name.
                DefaultDestinationTableName = table;

                // Get the field count.  This is the number of
                // public properties there are.
                PropertyInfo[] properties = type.GetProperties(
                    BindingFlags.Instance | BindingFlags.Public);

                // Set the field count.
                CachedFieldCount = properties.Length;

                // Set the mappings.
                OrdinalToMappingMap = (
                    from property in properties.Select((p, i) => new { Index = i, Property = p })
                    let attr = property.Property.GetCustomAttributes(typeof(ColumnAttribute), true).
                        Cast<ColumnAttribute>().SingleOrDefault()
                    let index = property.Index
                    let column = attr == null ? property.Property.Name : attr.Name
                    select new { Index = index, Column = column, property.Property }
                ).ToDictionary(p => p.Index,
                    p => new Mapping(p.Property, p.Column,
                        CreateCompiledPropertyAccessor(p.Property)));

                // Create the column to mapping map.
                ColumnToMappingMap = OrdinalToMappingMap.Values.ToDictionary(m => m.Column);
            }

            private static Func<T, object> CreateCompiledPropertyAccessor(PropertyInfo propertyInfo)
            {
                // validate the parameters.
                Debug.Assert(propertyInfo != null);

                // Create the parameter lambda.
                ParameterExpression pe = Expression.Parameter(typeof(T), "t");

                // Create the property accessor.
                MemberExpression me = Expression.Property(pe, propertyInfo);

                // Convert to object.
                UnaryExpression ue = Expression.Convert(me, typeof(object));

                // Create the compiled lambda and get out.
                return Expression.Lambda<Func<T, object>>(ue, pe).Compile();
            }

            #endregion

            #region Exception helpers.

            /// <summary>Gets a <see cref="SqlBulkCopyColumnIdException"/>
            /// given an <see cref="Exception"/> that is thrown
            /// during a call to
            /// <see cref="SqlBulkCopy.WriteToServer(DbDataReader)"/>.</summary>
            /// <param name="e">The <see cref="Exception"/> that was thrown.</param>
            /// <param name="connection">The <see cref="SqlConnection"/>
            /// that is used to get column information if not already
            /// retrieved.</param>
            /// <param name="table">The name of the table that was used as
            /// the destination for the bulk copy operation.</param>
            /// <returns>The <see cref="SqlBulkCopyColumnIdException"/>
            /// that contains information about the error in the bulk copy, or
            /// null if it can't be determined.</returns>
            internal static SqlBulkCopyColumnIdException GetBulkCopyColumnIdException(Exception e, SqlConnection connection, string table)
            {
                // Validate parameters.
                Debug.Assert(e != null);
                Debug.Assert(connection != null);
                Debug.Assert(!string.IsNullOrWhiteSpace(table));

                // The match on the regex.
                Match match;

                // Does the colid regex match?
                if (!(match = ColIdRegEx.Match(e.Message)).Success) return null;

                // It does.  Get the columns for the table.
                IEnumerable<string> reader = connection.Query<string>(
                    "select name from sys.columns where object_id = object_id({table}) order by column_id",
                    new { table = FormatTable(table) });

                // Remove the items from the columns that are not in the mappings (they
                // are already presented in order.
                // This is per:
                // http://eyeglazer.blogspot.com/2010/07/sqlbulkcopy-and-colid-error.html

                // The list of items that are in the table, and mapped.

                // The mapping.
                Mapping mapping = null;

                // Get the mapped columns (where the column name and the mapping match).
                IList<Mapping> mappedColumns = (
                    from column in reader
                    where ColumnToMappingMap.TryGetValue(column, out mapping)
                    select mapping
                ).ToList();

                // If the list has no items, get out, there's nothing that
                // can be done.
                if (mappedColumns.Count == 0) return null;

                // The column id group.
                Group columnIdGroup = match.Groups["columnId"];

                // It matched.
                Debug.Assert(columnIdGroup.Success);

                // The ordinal.
                int ordinal;

                // Try and parse the column id group into an integer.
                // If it fails, get out, nothing to do.
                if (!int.TryParse(columnIdGroup.Value, out ordinal)) return null;

                // Subtract one, as per link above.
                ordinal--;

                // If the range is invalid, get out.
                if (ordinal < 0 || ordinal >= mappedColumns.Count) return null;

                // Get the mapping.
                mapping = mappedColumns[ordinal];

                // Create the replacement text.
                string replacementText = string.Format(CultureInfo.CurrentCulture,
                    "column \"{mapping.Column}\" (Property: \"{1}\")", mapping.Column, mapping.Property.Name);

                // Replace in the regex and return a new exception.
                return new SqlBulkCopyColumnIdException(ColIdRegEx.Replace(e.Message, replacementText),
                    e, mapping.Property.Name, mapping.Column);

                // Return null.
            }

            private static string FormatTable(string table)
            {
                // Validate parameters.
                Debug.Assert(!string.IsNullOrWhiteSpace(table));

                // Parse apart the table.
                string[] parts = table.Split('.');

                // The builder.  Account for the brackets around each part.
                var builder = new StringBuilder(table.Length + (parts.Length * 2) + 1);

                // Surround each part with brackets.
                foreach (string part in parts)
                {
                    // Append.
                    builder.Append('[').Append(part).Append("].");
                }

                // Remove the last separator.
                builder.Length--;

                // Return the string.
                return builder.ToString();
            }

            private const string InvalidColumnMappingsExceptionMessage =
                "The given ColumnMapping does not match up with any column in the source or destination.";

            /// <summary>Gets a <see cref="SqlBulkCopyInvalidColumnMappingsException"/>
            /// given an <see cref="Exception"/> that is thrown
            /// during a call to
            /// <see cref="SqlBulkCopy.WriteToServer(DbDataReader)"/>.</summary>
            /// <param name="e">The <see cref="Exception"/> that was thrown.</param>
            /// <param name="sqlBulkCopy">The <see cref="SqlBulkCopy"/>
            /// instance that has </param>
            /// <remarks>This is extremely fragile, in that it relies on
            /// the <see cref="Exception.Message"/>; if the locale settings
            /// are different, then the match will not succeed.</remarks>
            /// <returns>The <see cref="SqlBulkCopyInvalidColumnMappingsException"/>
            /// that contains information about the error in the bulk copy mappings, or
            /// null if it can't be determined.</returns>
            internal static SqlBulkCopyInvalidColumnMappingsException GetBulkCopyInvalidColumnMappingsException(Exception e,
                SqlBulkCopy sqlBulkCopy)
            {
                // Validate parameters.
                Debug.Assert(e != null);
                Debug.Assert(sqlBulkCopy != null);

                // If the exception is not an InvalidOperationException and it doesn't have the message, get
                // out.
                // NOTE: This will break if the locale is different.
                var ioe = e as InvalidOperationException;

                // Check the exception, if not what we want, then get out.
                if (!(ioe != null && ioe.Message == InvalidColumnMappingsExceptionMessage)) return null;

                // Start reflecting.
                // NOTE: This is some deep, dark, dark magic.
                // The array list which holds the succesfully mapped fields.
                IEnumerable<object> goodMappings = ((IEnumerable)
                    SortedColumnMappingsFieldInfo.GetValue(sqlBulkCopy)).Cast<object>();

                // Copy the dictionary of mappings.
                IDictionary<string, Mapping> badMappings = new Dictionary<string, Mapping>(ColumnToMappingMap);

                // Get the enumerator for the good mappings.
                using (IEnumerator<object> enumerator = goodMappings.GetEnumerator())
                {
                    // Read the first item.
                    bool read = enumerator.MoveNext();

                    // There was an object read.
                    Debug.Assert(read);

                    // Get the field info.
                    FieldInfo metadataFieldInfo = enumerator.Current.GetType().GetField("_metadata",
                        BindingFlags.Instance | BindingFlags.NonPublic);

                    // The metadata field info is not null.
                    Debug.Assert(metadataFieldInfo != null);

                    // Get the column info on the column field.
                    FieldInfo columnFieldInfo = metadataFieldInfo.FieldType.GetField("column",
                        BindingFlags.Instance | BindingFlags.NonPublic);

                    // The column field info is not null.
                    Debug.Assert(columnFieldInfo != null);

                    // Process.
                    do
                    {
                        // The current value.
                        object current = enumerator.Current;

                        // Get the metadata.
                        object metadata = metadataFieldInfo.GetValue(current);

                        // The object is not null.
                        Debug.Assert(metadata != null);

                        // Get the column.
                        var column = (string)columnFieldInfo.GetValue(metadata);

                        // Remove the column.
                        badMappings.Remove(column);

                    } while (enumerator.MoveNext());
                }

                // Create the message.
                var builder = new StringBuilder().AppendLine();

                // Cycle through the mappings.
                foreach (Mapping mapping in badMappings.Values)
                {
                    // Add.
                    builder.AppendLine().AppendFormat(CultureInfo.CurrentCulture,
                        "Column: {0}, Property: {1}", mapping.Column, mapping.Property.Name);
                }

                // Create the exception and return.
                return new SqlBulkCopyInvalidColumnMappingsException(string.Format(CultureInfo.CurrentCulture,
                    "Invalid mappings found between the table {0} in the database and the properties on the type {1}:" +
                        builder,
                    sqlBulkCopy.DestinationTableName, typeof(T).FullName), e,
                    badMappings.Select(m => new KeyValuePair<string, PropertyInfo>(m.Key, m.Value.Property)));
            }

            /// <summary>Gets the <see cref="SqlBulkCopyInvalidConversionException"/>
            /// that is thrown when an invalid conversion occurs.</summary>
            /// <param name="e">The <see cref="Exception"/>
            /// that was thrown.</param>
            /// <param name="reader">The <see cref="BulkCopyDataReader{T}"/>
            /// instance that contains extra information about the
            /// items being yielded for bulk copy.</param>
            /// <returns>The <see cref="SqlBulkCopyInvalidConversionException"/>
            /// that should be thrown, or null if not applicable.</returns>
            internal static SqlBulkCopyInvalidConversionException GetBulkCopyInvalidConversionException(Exception e,
                BulkCopyDataReader<T> reader)
            {
                // Validate parameters.
                Debug.Assert(e != null);
                Debug.Assert(reader != null);

                // Get the invalid operation exception.
                var ioe = e as InvalidOperationException;

                // If null, get out.
                if (ioe == null) return null;

                // The regular expression match.
                Match match = InvalidConversionRegex.Match(ioe.Message);

                // Match exception message.
                if (!match.Success) return null;

                // Get the row and the item from the reader, and feed into the exception.
                return new SqlBulkCopyInvalidConversionException(ioe.Message, ioe, reader._enumerator.Current,
                    reader._itemsRead, match.Groups["sourceType"].Value, match.Groups["destinationType"].Value);
            }

            #endregion

            #region Instance-level state

            /// <summary>The number of the last item read in the sequence.</summary>
            /// <remarks>Starts at -1 in order to have a zero-based ordinal.</remarks>
            private int _itemsRead = -1;

            #endregion

            internal BulkCopyDataReader(IEnumerable<T> items)
            {
                // Validate parameters.
                Debug.Assert(items != null);

                // Set the enumerator.
                _enumerator = items.GetEnumerator();
            }

            private readonly IEnumerator<T> _enumerator;

            /// <summary>Advances the reader to the next record in a result set.</summary>
            /// <returns>true if there are more rows; otherwise false.</returns>
            public override bool Read()
            {
                // The result.
                bool result = _enumerator.MoveNext();

                // If there is nothing left, then close.
                if (!result) using (this) { }

                // Increment the row number, an item was read.
                _itemsRead++;

                // Indicate if there is a new item.
                return result;
            }

            public override Task<bool> ReadAsync(CancellationToken cancellationToken)
            {
                // Call the read implementation, no async operations here.
                return Task.FromResult(Read());
            }

            #region Overrides of DbDataReader

            /// <summary>Gets the number of columns in the current row.</summary>
            /// <returns>The number of columns in the current row.</returns>
            /// <exception cref="T:System.NotSupportedException">There is no current connection to an instance of SQL Server. </exception>
            public override int FieldCount => CachedFieldCount;

            #endregion

            public override object GetValue(int i)
            {
                // Get the lambda which will return
                // the value and return that.
                return OrdinalToMappingMap[i].PropertyAccessor(_enumerator.Current);
            }

            #region IDisposable implementation

            #region Overrides of DbDataReader

            protected override void Dispose(bool disposing)
            {
                // Call the base.
                base.Dispose(disposing);

                // If not disposing, return.
                if (!disposing) return;

                // Dispose of IDisposable implementations.
                // Dispose of the enumerator.
                using (_enumerator) { }
            }

            #endregion

            #endregion

            #region Not implemented

            #region Overrides of DbDataReader

            #region Overrides of DbDataReader

            public override IEnumerator GetEnumerator()
            {
                throw new NotImplementedException();
            }

            #endregion

            public override bool HasRows { get { throw new NotImplementedException(); } }

            #endregion

            public override int RecordsAffected
            {
                get { throw new NotImplementedException(); }
            }

            public override bool GetBoolean(int i)
            {
                throw new NotImplementedException();
            }

            public override byte GetByte(int i)
            {
                throw new NotImplementedException();
            }

            public override long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
            {
                throw new NotImplementedException();
            }

            public override char GetChar(int i)
            {
                throw new NotImplementedException();
            }

            public override long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
            {
                throw new NotImplementedException();
            }

            public override string GetDataTypeName(int i)
            {
                throw new NotImplementedException();
            }

            public override DateTime GetDateTime(int i)
            {
                throw new NotImplementedException();
            }

            public override decimal GetDecimal(int i)
            {
                throw new NotImplementedException();
            }

            public override double GetDouble(int i)
            {
                throw new NotImplementedException();
            }

            public override Type GetFieldType(int i)
            {
                throw new NotImplementedException();
            }

            public override float GetFloat(int i)
            {
                throw new NotImplementedException();
            }

            public override Guid GetGuid(int i)
            {
                throw new NotImplementedException();
            }

            public override short GetInt16(int i)
            {
                throw new NotImplementedException();
            }

            public override int GetInt32(int i)
            {
                throw new NotImplementedException();
            }

            public override long GetInt64(int i)
            {
                throw new NotImplementedException();
            }

            public override string GetName(int i)
            {
                throw new NotImplementedException();
            }

            public override int GetOrdinal(string name)
            {
                throw new NotImplementedException();
            }

            public override string GetString(int i)
            {
                throw new NotImplementedException();
            }

            public override int Depth
            {
                get { throw new NotImplementedException(); }
            }

            public override bool IsClosed
            {
                get { throw new NotImplementedException(); }
            }

            public override bool NextResult()
            {
                throw new NotImplementedException();
            }

            public override int GetValues(object[] values)
            {
                throw new NotImplementedException();
            }

            public override bool IsDBNull(int i)
            {
                throw new NotImplementedException();
            }

            public override object this[string name]
            {
                get { throw new NotImplementedException(); }
            }

            public override object this[int i]
            {
                get { throw new NotImplementedException(); }
            }
            #endregion
        }
    }
}
