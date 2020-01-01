using Dapper;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dommel
{
    public class SqlQuery<TEntityType> :
       SqlQuery
       where TEntityType : class
    {
        public SqlQuery(DommelMapper.ISqlBuilder sqlBuilder, string? alias = null, dynamic? parameters = null)
            : base(sqlBuilder, alias == null ? DommelMapper.Resolvers.Table(typeof(TEntityType), sqlBuilder) : $"{DommelMapper.Resolvers.Table(typeof(TEntityType), sqlBuilder) } {alias}")
        {
            _types.Add(typeof(TEntityType));
        }
    }

    public class SqlQuery
    {
        protected List<string> _splitOn;
        protected List<Type> _types;

        public DynamicParameters Parameters { get; set; }
        protected DommelMapper.ISqlBuilder DommelSqlBuilder { get; set; }
        protected Dapper.SqlBuilder SqlBuilder { get; set; }
        protected Dapper.SqlBuilder.Template QueryTemplate { get; set; }

        public SqlQuery(DommelMapper.ISqlBuilder sqlBuilder, string from, dynamic? parameters = null)
        {
            _splitOn = new List<string>();
            _types = new List<Type>();
            DommelSqlBuilder = sqlBuilder;
            Parameters = new DynamicParameters(parameters);
            SqlBuilder = new Dapper.SqlBuilder();

            // See https://github.com/StackExchange/Dapper/blob/master/Dapper.SqlBuilder/SqlBuilder.cs
            QueryTemplate = SqlBuilder.AddTemplate($@"SELECT
/**select**/ FROM {from}
/**innerjoin**//**leftjoin**//**rightjoin**//**join**/
/**where**//**orderby**/");
        }

        /// <summary>
        /// Adds a WHERE clause to the query, joining it with the previous with an 'AND' operator if needed.
        /// </summary>
        /// <remarks>
        /// Do not include the 'WHERE' keyword, as it is added automatically.
        /// </remarks>
        /// <example>
        ///     var queryBuilder = new SqlQueryBuilder();
        ///     queryBuilder.From("Customer customer");
        ///     queryBuilder.Select(
        ///         "customer.id",
        ///         "customer.name",
        ///     );
        ///     queryBuilder.SplitOn<Customer>("id");
        ///     queryBuilder.Where("customer.id == @id");
        ///     queryBuilder.Parameters.Add("id", 1);
        ///     var customer = queryBuilder
        ///         .Execute<Customer>(dbConnection, graphQLSelectionSet);
        ///         .FirstOrDefault();
        ///
        ///     // SELECT customer.id, customer.name
        ///     // FROM Customer customer
        ///     // WHERE customer.id == @id
        /// </example>
        /// <param name="where">An array of WHERE clauses.</param>
        /// <returns>The query builder.</returns>
        public SqlQuery AndWhere(string where, dynamic? parameters = null)
        {
            Parameters.AddDynamicParams(parameters);
            SqlBuilder.Where(where);
            return this;
        }

        /*
        /// <summary>
        /// Executes the query with Dapper, using the provided database connection and map function.
        /// </summary>
        /// <example>
        ///     var queryBuilder = new SqlQueryBuilder();
        ///     queryBuilder.From("Customer customer");
        ///     queryBuilder.Select(
        ///         "customer.id",
        ///         "customer.name",
        ///     );
        ///     queryBuilder.SplitOn<Customer>("id");
        ///     queryBuilder.Where("customer.id == @id");
        ///     queryBuilder.Parameters.Add("id", 1);
        ///     var customer = queryBuilder
        ///         .Execute<Customer>(dbConnection, graphQLSelectionSet)
        ///         .FirstOrDefault();
        ///
        ///     // SELECT customer.id, customer.name
        ///     // FROM Customer customer
        ///     // WHERE customer.id == @id
        /// </example>
        /// <typeparam name="TEntityType">The entity type to be mapped.</typeparam>
        /// <param name="connection">The database connection.</param>
        /// <param name="mapper">The entity mapper.</param>
        /// <param name="selectionSet">The GraphQL selection set (optional).</param>
        /// <param name="transaction">The transaction to execute under (optional).</param>
        /// <param name="options">The options for the query (optional).</param>
        /// <returns>A list of entities returned by the query.</returns>
        public IEnumerable<TEntityType> Execute<TEntityType>(
            IDbConnection connection,
            IHaveSelectionSet selectionSet,
            IEntityMapper<TEntityType> mapper = null,
            IDbTransaction transaction = null,
            SqlMapperOptions options = null)
            where TEntityType : class
        {
            if (options == null)
            {
                options = SqlMapperOptions.DefaultOptions;
            }

            if (mapper == null)
            {
                mapper = new EntityMapper<TEntityType>();
            }

            // Build function that uses a mapping context to map our entities
            var fn = new Func<object[], TEntityType>(objs =>
            {
                var context = new EntityMapContext
                {
                    Items = objs,
                    SelectionSet = selectionSet,
                    SplitOn = GetSplitOnTypes(),
                };
                using (context)
                {
                    return mapper.Map(context);
                }
            });

            var results = connection.Query<TEntityType>(
                sql: this.ToString(),
                types: this._types.ToArray(),
                param: this.Parameters,
                map: fn,
                splitOn: string.Join(",", this._splitOn),
                transaction: transaction,
                commandTimeout: options.CommandTimeout,
                commandType: options.CommandType,
                buffered: options.Buffered
            );
            return results.Where(e => e != null);
        }


        /// <summary>
        /// Executes the query with Dapper asynchronously, using the provided database connection and map function.
        /// </summary>
        /// <example>
        ///     var queryBuilder = new SqlQueryBuilder();
        ///     queryBuilder.From("Customer customer");
        ///     queryBuilder.Select(
        ///         "customer.id",
        ///         "customer.name",
        ///     );
        ///     queryBuilder.SplitOn<Customer>("id");
        ///     queryBuilder.Where("customer.id == @id");
        ///     queryBuilder.Parameters.Add("id", 1);
        ///     var customer = queryBuilder
        ///         .Execute<Customer>(dbConnection, graphQLSelectionSet)
        ///         .FirstOrDefault();
        ///
        ///     // SELECT customer.id, customer.name
        ///     // FROM Customer customer
        ///     // WHERE customer.id == @id
        /// </example>
        /// <typeparam name="TEntityType">The entity type to be mapped.</typeparam>
        /// <param name="connection">The database connection.</param>
        /// <param name="mapper">The entity mapper.</param>
        /// <param name="selectionSet">The GraphQL selection set (optional).</param>
        /// <param name="transaction">The transaction to execute under (optional).</param>
        /// <param name="options">The options for the query (optional).</param>
        /// <returns>A list of entities returned by the query.</returns>
        public async Task<IEnumerable<TEntityType>> ExecuteAsync<TEntityType>(
            IDbConnection connection,
            IHaveSelectionSet selectionSet,
            IEntityMapper<TEntityType> mapper = null,
            IDbTransaction transaction = null,
            SqlMapperOptions options = null)
            where TEntityType : class
        {
            if (options == null)
            {
                options = SqlMapperOptions.DefaultOptions;
            }

            if (mapper == null)
            {
                mapper = new EntityMapper<TEntityType>();
            }

            // Build function that uses a mapping context to map our entities
            var fn = new Func<object[], TEntityType>(objs =>
            {
                var context = new EntityMapContext
                {
                    Items = objs,
                    SelectionSet = selectionSet,
                    SplitOn = GetSplitOnTypes(),
                };
                using (context)
                {
                    return mapper.Map(context);
                }
            });

            var results = await connection.QueryAsync<TEntityType>(
                sql: this.ToString(),
                types: this._types.ToArray(),
                param: this.Parameters,
                map: fn,
                splitOn: string.Join(",", this._splitOn),
                transaction: transaction,
                commandTimeout: options.CommandTimeout,
                commandType: options.CommandType,
                buffered: options.Buffered
            );
            return results.Where(e => e != null);
        }
        */

        /// <summary>
        /// Gets an array of types that are used to split objects during entity mapping.
        /// </summary>
        /// <returns></returns>
        public List<Type> GetSplitOnTypes()
        {
            return _types;
        }

        /// <summary>
        /// Performs an INNER JOIN.
        /// </summary>
        /// <remarks>
        /// Do not include the 'INNER JOIN' keywords, as they are added automatically.
        /// </remarks>
        /// <example>
        ///     var queryBuilder = new SqlQueryBuilder();
        ///     queryBuilder.From("Customer customer");
        ///     queryBuilder.InnerJoin("Account account ON customer.Id = account.CustomerId");
        ///     queryBuilder.Select(
        ///         "customer.id",
        ///         "account.id",
        ///     );
        ///     queryBuilder.SplitOn<Customer>("id");
        ///     queryBuilder.SplitOn<Account>("id");
        ///     queryBuilder.Where("customer.id == @id");
        ///     queryBuilder.Parameters.Add("id", 1);
        ///     var customer = queryBuilder
        ///         .Execute<Customer>(dbConnection, graphQLSelectionSet);
        ///         .FirstOrDefault();
        ///
        ///     // SELECT customer.id, account.id
        ///     // FROM
        ///     //     Customer customer INNER JOIN
        ///     //     Account account ON customer.Id = account.CustomerId
        ///     // WHERE customer.id == @id
        /// </example>
        /// <param name="join">The INNER JOIN clause.</param>
        /// <param name="parameters">Parameters included in the statement.</param>
        /// <returns>The query builder.</returns>
        public SqlQuery InnerJoin<TEntity>(string? join = null, dynamic? parameters = null)
        {
            // RemoveSingleTableQueryItems();
            var type = typeof(TEntity);
            _types.Add(type);
            Parameters.AddDynamicParams(parameters);
            if (join == null)
            {
                join = ComputeJoinString(type);
            }
            SqlBuilder.InnerJoin(join);
            return this;
        }
        
        private string ComputeJoinString(Type type)
        {
            // ForeignKey single is all that is currently supported.
            var foreignProp = DommelMapper.Resolvers.ForeignKeyProperty(_types.First(), type, out var relation);
            var keyProps = DommelMapper.Resolvers.KeyProperties(type);
            if (keyProps.Length > 1)
            {
                throw new InvalidOperationException($"Number of key columns of {type.Name} exceeds the number that is currently supported.  Composite keys not supported automatically.");
            }
            var idProp = keyProps.FirstOrDefault();
            var joinTable = DommelMapper.Resolvers.Table(type, DommelSqlBuilder);
            var table = DommelMapper.Resolvers.Table(_types.First(), DommelSqlBuilder);
            return $"{joinTable} ON {joinTable}.{DommelMapper.Resolvers.Column(idProp.Property, DommelSqlBuilder)} = {table}.{DommelMapper.Resolvers.Column(foreignProp, DommelSqlBuilder)}";
        }

        /// <summary>
        /// Performs a LEFT OUTER JOIN.
        /// </summary>
        /// <remarks>
        /// Do not include the 'LEFT OUTER JOIN' keywords, as they are added automatically.
        /// </remarks>
        /// <example>
        ///     var queryBuilder = new SqlQueryBuilder();
        ///     queryBuilder.From("Customer customer");
        ///     queryBuilder.LeftOuterJoin("Account account ON customer.Id = account.CustomerId");
        ///     queryBuilder.Select(
        ///         "customer.id",
        ///         "account.id",
        ///     );
        ///     queryBuilder.SplitOn<Customer>("id");
        ///     queryBuilder.SplitOn<Account>("id");
        ///     queryBuilder.Where("customer.id == @id");
        ///     queryBuilder.Parameters.Add("id", 1);
        ///     var customer = queryBuilder
        ///         .Execute<Customer>(dbConnection, graphQLSelectionSet);
        ///         .FirstOrDefault();
        ///
        ///     // SELECT customer.id, account.id
        ///     // FROM
        ///     //     Customer customer LEFT OUTER JOIN
        ///     //     Account account ON customer.Id = account.CustomerId
        ///     // WHERE customer.id == @id
        /// </example>
        /// <param name="join">The LEFT JOIN clause.</param>
        /// <param name="parameters">Parameters included in the statement.</param>
        /// <returns>The query builder.</returns>
        public SqlQuery LeftJoin<TEntity>(string? join = null, dynamic? parameters = null)
        {
            // RemoveSingleTableQueryItems();
            var type = typeof(TEntity);
            _types.Add(type);
            Parameters.AddDynamicParams(parameters);
            if (join == null)
            {
                join = ComputeJoinString(type);
            }
            SqlBuilder.LeftJoin(join);
            return this;
        }

        /// <summary>
        /// Adds an ORDER BY clause to the end of the query.
        /// </summary>
        /// <remarks>
        /// Do not include the 'ORDER BY' keywords, as they are added automatically.
        /// </remarks>
        /// <example>
        ///     var queryBuilder = new SqlQueryBuilder();
        ///     queryBuilder.From("Customer customer");
        ///     queryBuilder.Select(
        ///         "customer.id",
        ///         "customer.name",
        ///     );
        ///     queryBuilder.SplitOn<Customer>("id");
        ///     queryBuilder.Where("customer.id == @id");
        ///     queryBuilder.Parameters.Add("id", 1);
        ///     queryBuilder.Orderby("customer.name");
        ///     var customer = queryBuilder
        ///         .Execute<Customer>(dbConnection, graphQLSelectionSet);
        ///         .FirstOrDefault();
        ///
        ///     // SELECT customer.id, customer.name
        ///     // FROM Customer customer
        ///     // WHERE customer.id == @id
        ///     // ORDER BY customer.name
        /// </example>
        /// <param name="orderBy">One or more GROUP BY clauses.</param>
        /// <param name="parameters">Parameters included in the statement.</param>
        /// <returns>The query builder.</returns>
        public SqlQuery OrderBy(string orderBy, dynamic parameters = null)
        {
            Parameters.AddDynamicParams(parameters);
            SqlBuilder.OrderBy(orderBy);
            return this;
        }

        /// <summary>
        /// Adds a WHERE clause to the query, joining it with the previous with an 'OR' operator if needed.
        /// </summary>
        /// <remarks>
        /// Do not include the 'WHERE' keyword, as it is added automatically.
        /// </remarks>
        /// <param name="where">An array of WHERE clauses.</param>
        /// <param name="parameters">Parameters included in the statement.</param>
        /// <returns>The query builder.</returns>
        public SqlQuery OrWhere(string where, dynamic? parameters = null)
        {
            Parameters.AddDynamicParams(parameters);
            SqlBuilder.OrWhere(where);
            return this;
        }

        /// <summary>
        /// Adds a SELECT statement to the query, joining it with previous items already selected.
        /// </summary>
        /// <remarks>
        /// Do not include the 'SELECT' keyword, as it is added automatically.
        /// </remarks>
        /// <example>
        ///     var queryBuilder = new SqlQueryBuilder();
        ///     var customer = queryBuilder
        ///         .From("Customer customer")
        ///         .Select(
        ///            "customer.id",
        ///            "customer.name",
        ///         )
        ///         .SplitOn<Customer>("id")
        ///         .Where("customer.id == @id")
        ///         .WithParameter("id", 1)
        ///         .Execute<Customer>(dbConnection, graphQLSelectionSet);
        ///         .FirstOrDefault();
        ///
        ///     // SELECT customer.id, customer.name
        ///     // FROM Customer customer
        ///     // WHERE customer.id == @id
        /// </example>
        /// <param name="select">The column to select.</param>
        /// <param name="parameters">Parameters included in the statement.</param>
        /// <returns>The query builder.</returns>
        public SqlQuery Select<TEntity>(string select, dynamic? parameters = null)
        {
            Parameters.AddDynamicParams(parameters);

            var type = typeof(TEntity);
            AddColumn(type, select);
            return this;
        }

        public SqlQuery Select<TEntity>(params string[] select)
        {
            var type = typeof(TEntity);

            foreach (var s in select)
            {
                AddColumn(type, s);
            }
            return this;
        }

        private void AddColumn(Type type, string propertyName)
        {
            var propertyMaps = DommelMapper.Resolvers.Properties(type);
            var propertyInfo = propertyMaps.First(x => x.Name.Equals(propertyName, StringComparison.OrdinalIgnoreCase));

            SqlBuilder.Select($"{DommelMapper.Resolvers.Table(type, DommelSqlBuilder)}.{DommelMapper.Resolvers.Column(propertyInfo, DommelSqlBuilder)}");
        }

        /// <summary>
        /// Instructs dapper to deserialized data into a different type, beginning with the specified column.
        /// </summary>
        /// <typeparam name="TEntityType">The type to map data into.</typeparam>
        /// <param name="columnName">The name of the column to map into a different type.</param>
        /// <see cref="http://dapper-tutorial.net/result-multi-mapping" />
        /// <returns>The query builder.</returns>
        public SqlQuery SplitOn<TEntityType>(string columnName)
        {
            return SplitOn(columnName, typeof(TEntityType));
        }

        /// <summary>
        /// Instructs dapper to deserialized data into a different type, beginning with the specified column.
        /// </summary>
        /// <param name="columnName">The name of the column to map into a different type.</param>
        /// <param name="entityType">The type to map data into.</param>
        /// <see cref="http://dapper-tutorial.net/result-multi-mapping" />
        /// <returns>The query builder.</returns>
        public SqlQuery SplitOn(string columnName, Type entityType)
        {
            // RemoveSingleTableQueryItems();

            _splitOn.Add(columnName);
            _types.Add(entityType);

            return this;
        }

        /// <summary>
        /// Renders the generated SQL statement.
        /// </summary>
        /// <returns>The rendered SQL statement.</returns>
        public override string ToString()
        {
            return QueryTemplate.RawSql;
        }

        public string ToSql()
        {
            return ToString();

        }
        /// <summary>
        /// An alias for AndWhere().
        /// </summary>
        /// <param name="where">The WHERE clause.</param>
        /// <param name="parameters">Parameters included in the statement.</param>
        public SqlQuery Where(string where, dynamic parameters = null)
        {
            return AndWhere(where, parameters);
        }

        /// <summary>
        /// Clears out items that are only relevant for single-table queries.
        /// </summary>
        private void RemoveSingleTableQueryItems()
        {
            if (_types.Count > 0 && _splitOn.Count == 0)
            {
                _types.Clear();
            }
        }
    }
}
