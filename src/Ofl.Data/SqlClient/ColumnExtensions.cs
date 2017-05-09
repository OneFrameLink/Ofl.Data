using System;
using System.ComponentModel.DataAnnotations.Schema;
using System.Reflection;

namespace Ofl.Data.SqlClient
{
    public static class ColumnExtensions
    {
        public static string GetColumnMapping(this PropertyInfo propertyInfo)
        {
            // Validate parameters.
            if (propertyInfo == null) throw new ArgumentNullException(nameof(propertyInfo));

            // Is it not mapped?
            if (propertyInfo.GetCustomAttribute<NotMappedAttribute>(true) != null) return null;

            // Column attribute?
            var columnAttribute = propertyInfo.GetCustomAttribute<ColumnAttribute>(true);

            // If not null, return that.
            return columnAttribute == null ? propertyInfo.Name : columnAttribute.Name;
        }
    }
}
