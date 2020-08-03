using System;
using System.Linq;
using System.Reflection;

namespace Dommel
{
    /// <summary>
    /// <see cref="ISqlBuilder"/> implementation for SQL Server.
    /// </summary>
    public class SqlServerSqlBuilder : ISqlBuilder
    {
        /// <inheritdoc/>
        public virtual string BuildInsert(Type type, string tableName, string[] columnNames, string[] paramNames) =>
            $"set nocount on insert into {tableName} ({string.Join(", ", columnNames)}) values ({string.Join(", ", paramNames)}); select scope_identity()";

        /// <inheritdoc/>
        public virtual string BuildPaging(string? orderBy, int pageNumber, int pageSize)
        {
            var start = pageNumber >= 1 ? (pageNumber - 1) * pageSize : 0;
            return $" {orderBy} offset {start} rows fetch next {pageSize} rows only";
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
        public string QuoteIdentifier(string identifier) => $"[{identifier}]";

        /// <inheritdoc/>
        public string LimitClause(int count) => $"order by 1 offset 0 rows fetch next {count} rows only";
    }
}
