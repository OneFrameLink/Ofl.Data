using System;
using System.Globalization;
using System.Linq;

namespace Ofl.Data.SqlClient
{
    public static class StringExtensions
    {
        public static string AsBracketedIdentifier(this string s)
        {
            // Validate parameters.
            if (string.IsNullOrWhiteSpace(s)) throw new ArgumentNullException(nameof(s));

            // Split the string on .
            string[] parts = s.Split(new [] {'.'}, StringSplitOptions.RemoveEmptyEntries);

            // Add each.
            return string.Join(".", parts.Select(
                p => p.StartsWith("[") && p.EndsWith("]") ? 
                    p : 
                    string.Format(CultureInfo.InvariantCulture, "[{0}]", p)));
        }
    }
}
