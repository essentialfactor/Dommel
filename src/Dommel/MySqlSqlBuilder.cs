using System;
using System.Linq;
using System.Reflection;

namespace Dommel
{
    /// <summary>
    /// <see cref="ISqlBuilder"/> implementation for MySQL.
    /// </summary>
    public class MySqlSqlBuilder : ISqlBuilder
    {
        /// <inheritdoc/>
        public virtual string BuildInsert(Type type, string tableName, string[] columnNames, string[] paramNames) =>
            $"insert into {tableName} ({string.Join(", ", columnNames)}) values ({string.Join(", ", paramNames)}); select LAST_INSERT_ID() id";

        /// <inheritdoc/>
        public virtual string BuildPaging(string? orderBy, int pageNumber, int pageSize)
        {
            var start = pageNumber >= 1 ? (pageNumber - 1) * pageSize : 0;
            return $" {orderBy} limit {start}, {pageSize}";
        }

        /// <inheritdoc/>
        public string BuildUpdate(Type type, string tableName, ColumnPropertyInfo[] properties, ColumnPropertyInfo[] keys)
        {
            var columnNames = properties.Select(p => $"{Resolvers.Column(p.Property, this)} = {PrefixParameter(p.Property.Name)}").ToArray();
            var whereClauses = keys.Select(p => $"{Resolvers.Column(p.Property, this)} = {PrefixParameter(p.Property.Name)}");
            return $"update {tableName} set {string.Join(", ", columnNames)} where {string.Join(" and ", whereClauses)}";
        }

        /// <inheritdoc/>
        public string BuildDelete(Type type, string tableName, ColumnPropertyInfo[] keys)
        {
            var whereClauses = keys.Select(p => $"{Resolvers.Column(p.Property, this)} = {PrefixParameter(p.Property.Name)}");
            return $"delete from {tableName} where {string.Join(" and ", whereClauses)}";
        }

        /// <inheritdoc/>
        public string PrefixParameter(string paramName) => $"@{paramName}";

        /// <inheritdoc/>
        public string QuoteIdentifier(string identifier) => $"`{identifier}`";

        /// <inheritdoc/>
        public string LimitClause(int count) => $"limit {count}";
    }
}
