﻿using System;
using System.Linq;
using System.Reflection;

namespace Dommel
{
    /// <summary>
    /// <see cref="ISqlBuilder"/> implementation for SQL Server Compact Edition.
    /// </summary>
    public class SqlServerCeSqlBuilder : ISqlBuilder
    {
        /// <inheritdoc/>
        public virtual string BuildInsert(Type type, string tableName, string[] columnNames, string[] paramNames) =>
            $"insert into {tableName} ({string.Join(", ", columnNames)}) values ({string.Join(", ", paramNames)}); select @@IDENTITY";

        /// <inheritdoc/>
        public virtual string BuildPaging(string? orderBy, int pageNumber, int pageSize)
        {
            var start = pageNumber >= 1 ? (pageNumber - 1) * pageSize : 0;
            return $" {orderBy} offset {start} rows fetch next {pageSize} rows only";
        }

        /// <inheritdoc/>
        public string BuildUpdate(Type type, string tableName, PropertyInfo[] properties, KeyPropertyInfo[] keys)
        {
            var columnNames = properties.Select(p => $"{Resolvers.Column(p, this)} = {PrefixParameter(p.Name)}").ToArray();
            var whereClauses = keys.Select(p => $"{Resolvers.Column(p.Property, this)} = {PrefixParameter(p.Property.Name)}");
            return $"update {tableName} set {string.Join(", ", columnNames)} where {string.Join(" and ", whereClauses)}";
        }

        /// <inheritdoc/>
        public string BuildDelete(Type type, string tableName, KeyPropertyInfo[] keys)
        {
            var whereClauses = keys.Select(p => $"{Resolvers.Column(p.Property, this)} = {PrefixParameter(p.Property.Name)}");
            return $"delete from {tableName} where {string.Join(" and ", whereClauses)}";
        }

        /// <inheritdoc/>
        public string PrefixParameter(string paramName) => $"@{paramName}";

        /// <inheritdoc/>
        public string QuoteIdentifier(string identifier) => $"[{identifier}]";
    }
}
