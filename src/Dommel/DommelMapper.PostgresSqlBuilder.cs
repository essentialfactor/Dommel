﻿using System;
using System.Reflection;

namespace Dommel
{
    public static partial class DommelMapper
    {
        private sealed class PostgresSqlBuilder : ISqlBuilder
        {
            public string BuildInsert(string tableName, string[] columnNames, string[] paramNames, PropertyInfo keyProperty)
            {
                var sql = $"insert into {tableName} ({string.Join(", ", columnNames)}) values ({string.Join(", ", paramNames)})";

                if (keyProperty != null)
                {
                    var keyColumnName = Resolvers.Column(keyProperty);

                    sql += " RETURNING " + keyColumnName;
                }
                else
                {
                    // todo: what behavior is desired here?
                    throw new Exception("A key property is required for the PostgresSqlBuilder.");
                }

                return sql;
            }
        }
    }
}
