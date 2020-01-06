using Dapper;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
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

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="TRelated"></typeparam>
        /// <param name="expression"></param>
        /// <returns></returns>
        public SqlQuery<TEntityType> InnerJoin<TRelated>(Expression<Func<TEntityType, TRelated, bool>>? expression = null)
        {
            var type = typeof(TRelated);
            _types.Add(type);

            var onSql = expression == null ? VisitExpression(ComputeJoinOnStatement(type)) : VisitExpression(expression).ToString();
            SqlBuilder.InnerJoin($"{DommelMapper.Resolvers.Table(type, DommelSqlBuilder)} ON {onSql}");
            
            return this;
        }

        public virtual SqlQuery<TEntityType> Select<TEntity>(params string[] propertyNames)
        {
            var type = typeof(TEntity);
            _types.Add(type);

            var propertyMaps = DommelMapper.Resolvers.Properties(type);
            var columns = propertyNames.Select(p => $"{DommelMapper.Resolvers.Table(type, DommelSqlBuilder)}.{DommelMapper.Resolvers.Column(propertyMaps.First(x => x.Name.Equals(p, StringComparison.OrdinalIgnoreCase)), DommelSqlBuilder)}");
            foreach (var propertyName in propertyNames)
            {
                AddColumn(type, propertyName);
            }
            return this;
        }

        public virtual SqlQuery<TEntityType> Select<TEntity>(Func<TEntity, object> selector)
        {
            if (selector == null)
            {
                throw new ArgumentNullException(nameof(selector));
            }
            var type = typeof(TEntity);

            if (!_types.Contains(type))
            {
                _types.Add(type);
            }

            var NewEntityFunc = Expression.Lambda<Func<TEntity>>(
                Expression.New(typeof(TEntity).GetConstructors()[0])).Compile();
            var obj = selector.Invoke(NewEntityFunc());

            var props = DommelMapper.Resolvers.Properties(obj.GetType());
            var columns = props.Select(p => $"{DommelMapper.Resolvers.Table(type, DommelSqlBuilder)}.{DommelMapper.Resolvers.Column(p, DommelSqlBuilder)}");

            SqlBuilder.Select(string.Join(", ", columns));

            return this;
        }

        public virtual SqlQuery<TEntityType> Where<TEntity>(Expression<Func<TEntity, bool>> whereExpression)
        {
            return Where(whereExpression as Expression);
        }

        public virtual SqlQuery<TEntityType> Where(Expression<Func<TEntityType, bool>> whereExpression)
        {
            return Where(whereExpression as Expression);
        }

        /// <summary>
        /// User supplied totally custom predicate expression.
        /// </summary>
        /// <param name="whereExpression"></param>
        /// <returns></returns>
        public virtual SqlQuery<TEntityType> Where(Expression whereExpression)
        {
            SqlBuilder.Where(VisitExpression(whereExpression).ToString());
            return this;
        }

        private void AddColumn(Type type, string propertyName)
        {
            var propertyMaps = DommelMapper.Resolvers.Properties(type);
            var propertyInfo = propertyMaps.First(x => x.Name.Equals(propertyName, StringComparison.OrdinalIgnoreCase));

            SqlBuilder.Select($"{DommelMapper.Resolvers.Table(type, DommelSqlBuilder)}.{DommelMapper.Resolvers.Column(propertyInfo, DommelSqlBuilder)}");
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
        private int _parameterIndex;

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
                join = $"{DommelMapper.Resolvers.Table(type, DommelSqlBuilder)} ON {VisitExpression(ComputeJoinOnStatement(type))}";
            }
            SqlBuilder.InnerJoin(join);
            return this;
        }
        
        protected Expression ComputeJoinOnStatement(Type type)
        {
            // ForeignKey single is all that is currently supported.
            var foreignProp = DommelMapper.Resolvers.ForeignKeyProperty(_types.First(), type, out var relation);
            var keyProps = DommelMapper.Resolvers.KeyProperties(type);
            if (keyProps.Length > 1)
            {
                throw new InvalidOperationException($"Number of key columns of {type.Name} exceeds the number that is currently supported.  Composite keys not supported automatically.");
            }
            var idProp = keyProps.FirstOrDefault();
            
            var rightParam = Expression.Parameter(_types.First(), _types.First().Name);
            var rightSide = Expression.MakeMemberAccess(rightParam, foreignProp);
            
            var leftParam = Expression.Parameter(type, type.Name);
            var leftSide = Expression.MakeMemberAccess(leftParam, idProp.Property);

            return Expression.MakeBinary(ExpressionType.Equal, leftSide, rightSide);
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
                join = $"{DommelMapper.Resolvers.Table(type, DommelSqlBuilder)} ON {VisitExpression(ComputeJoinOnStatement(type))}";
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

        public SqlQuery Select(string select, dynamic? parameters = null)
        {
            SqlBuilder.Select(select, parameters);
            return this;
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

        public string ToSql(out DynamicParameters dynamicParameters)
        {
            dynamicParameters = Parameters;
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

        /// <summary>
        /// Visits the expression.
        /// </summary>
        /// <param name="expression">The expression to visit.</param>
        /// <returns>The result of the visit.</returns>
        protected virtual object VisitExpression(Expression expression)
        {
            DynamicParameters parameters = new DynamicParameters();
            
            switch (expression.NodeType)
            {
                case ExpressionType.Lambda:
                    return VisitLambda((LambdaExpression)expression);

                case ExpressionType.LessThan:
                case ExpressionType.LessThanOrEqual:
                case ExpressionType.GreaterThan:
                case ExpressionType.GreaterThanOrEqual:
                case ExpressionType.Equal:
                case ExpressionType.NotEqual:
                case ExpressionType.And:
                case ExpressionType.AndAlso:
                case ExpressionType.Or:
                case ExpressionType.OrElse:
                    return VisitBinary((BinaryExpression)expression);

                case ExpressionType.Convert:
                case ExpressionType.Not:
                    return VisitUnary((UnaryExpression)expression);

                case ExpressionType.New:
                    return VisitNew((NewExpression)expression);

                case ExpressionType.MemberAccess:
                    return VisitMemberAccess((MemberExpression)expression);

                case ExpressionType.Constant:
                    return VisitConstantExpression((ConstantExpression)expression);
                case ExpressionType.Call:
                    return VisitCallExpression((MethodCallExpression)expression);
                case ExpressionType.Invoke:
                    return VisitExpression(((InvocationExpression)expression).Expression);
            }

            return expression;
        }

        /// <summary>
        /// Specifies the type of text search to use.
        /// </summary>
        protected enum TextSearch
        {
            /// <summary>
            /// Matches anywhere in a string.
            /// </summary>
            Contains,

            /// <summary>
            /// Matches the start of a string.
            /// </summary>
            StartsWith,

            /// <summary>
            /// Matches the end of a string.
            /// </summary>
            EndsWith
        }

        /// <summary>
        /// Process a method call expression.
        /// </summary>
        /// <param name="expression">The method call expression.</param>
        /// <returns>The result of the processing.</returns>
        protected virtual object VisitCallExpression(MethodCallExpression expression)
        {
            var method = expression.Method.Name.ToLower();
            switch (method)
            {
                case "contains":
                    // Is this a string-contains or array-contains expression?
                    if (expression.Object != null && expression.Object.Type == typeof(string))
                    {
                        return VisitContainsExpression(expression, TextSearch.Contains);
                    }
                    else
                    {
                        return VisitInExpression(expression);
                    }
                case "startswith":
                    return VisitContainsExpression(expression, TextSearch.StartsWith);
                case "endswith":
                    return VisitContainsExpression(expression, TextSearch.EndsWith);
                default:
                    break;
            }

            return expression;
        }

        /// <summary>
        /// Processes a contains expression as IN clause
        /// </summary>
        /// <param name="expression">The method call expression.</param>
        /// <returns>The result of the processing.</returns>
        protected virtual object VisitInExpression(MethodCallExpression expression)
        {
            Expression collection;
            Expression property;
            if (expression.Object == null && expression.Arguments.Count == 2)
            {
                // The method is a static method, and has 2 arguments.
                // usually, it's from System.Linq.Enumerable
                collection = expression.Arguments[0];
                property = expression.Arguments[1];
            }
            else if (expression.Object != null && expression.Arguments.Count == 1)
            {
                // The method is an instance method, and has only 1 argument.
                // usually, it's from System.Collections.IList
                collection = expression.Object;
                property = expression.Arguments[0];
            }
            else
            {
                throw new Exception("Unsupported method call: " + expression.Method.Name);
            }

            var inClause = new StringBuilder("(");
            foreach (var value in (System.Collections.IEnumerable)VisitMemberAccess((MemberExpression)collection))
            {
                AddParameter(value, out var paramName);
                inClause.Append($"{paramName},");
            }
            if (inClause.Length == 1)
            {
                inClause.Append("null,");
            }
            inClause[inClause.Length - 1] = ')';

            return $"{VisitExpression(property)} in {inClause}";
        }

        /// <summary>
        /// Processes a contains expression for string.
        /// </summary>
        /// <param name="expression">The method call expression.</param>
        /// <param name="textSearch">Type of search.</param>
        /// <returns>The result of the processing.</returns>
        protected virtual object VisitContainsExpression(MethodCallExpression expression, TextSearch textSearch)
        {
            var column = VisitExpression(expression.Object);
            if (expression.Arguments.Count == 0 || expression.Arguments.Count > 1)
            {
                throw new ArgumentException("Contains-expression should contain exactly one argument.", nameof(expression));
            }

            var value = VisitExpression(expression.Arguments[0]);
            var textLike = textSearch switch
            {
                TextSearch.Contains => $"%{value}%",
                TextSearch.StartsWith => $"{value}%",
                TextSearch.EndsWith => $"%{value}",
                _ => throw new ArgumentOutOfRangeException($"Invalid TextSearch value '{textSearch}'.", nameof(textSearch)),
            };
            AddParameter(textLike, out var paramName);
            return $"{column} like {paramName}";
        }

        /// <summary>
        /// Processes a lambda expression.
        /// </summary>
        /// <param name="epxression">The lambda expression.</param>
        /// <returns>The result of the processing.</returns>
        protected virtual object VisitLambda(LambdaExpression epxression)
        {
            if (epxression.Body.NodeType == ExpressionType.MemberAccess)
            {
                var member = epxression.Body as MemberExpression;
                if (member?.Expression != null)
                {
                    return $"{VisitMemberAccess(member)} = '1'";
                }
            }

            return VisitExpression(epxression.Body);
        }

        /// <summary>
        /// Processes a binary expression.
        /// </summary>
        /// <param name="expression">The binary expression.</param>
        /// <returns>The result of the processing.</returns>
        protected virtual object VisitBinary(BinaryExpression expression)
        {
            object left, right;
            var operand = GetOperant(expression.NodeType);
            if (operand == "and" || operand == "or")
            {
                // Process left and right side of the "and/or" expression, e.g.:
                // Foo == 42    or      Bar == 42
                //   left    operand     right
                //
                if (expression.Left is MemberExpression leftMember && leftMember.Expression?.NodeType == ExpressionType.Parameter)
                {
                    left = $"{VisitMemberAccess(leftMember)} = '1'";
                }
                else
                {
                    left = VisitExpression(expression.Left);
                }

                if (expression.Right is MemberExpression rightMember && rightMember.Expression?.NodeType == ExpressionType.Parameter)
                {
                    right = $"{VisitMemberAccess(rightMember)} = '1'";
                }
                else
                {
                    right = VisitExpression(expression.Right);
                }
            }
            else
            {
                // It's a single expression, e.g. Foo == 42
                left = VisitExpression(expression.Left);
                right = VisitExpression(expression.Right);

                if (right == null)
                {
                    // Special case 'is (not) null' syntax
                    if (expression.NodeType == ExpressionType.Equal)
                    {
                        return $"{left} is null";
                    }
                    else
                    {
                        return $"{left} is not null";
                    }
                }

                if (expression.Right is MemberExpression rightMember && rightMember.Expression?.NodeType == ExpressionType.Parameter)
                {
                    return $"{left} {operand} {right}";
                }
                AddParameter(right, out var paramName);
                return $"{left} {operand} {paramName}";
            }

            return $"{left} {operand} {right}";
        }

        /// <summary>
        /// Processes a unary expression.
        /// </summary>
        /// <param name="expression">The unary expression.</param>
        /// <returns>The result of the processing.</returns>
        protected virtual object VisitUnary(UnaryExpression expression)
        {
            switch (expression.NodeType)
            {
                case ExpressionType.Not:
                    var o = VisitExpression(expression.Operand);
                    if (!(o is string))
                    {
                        return !(bool)o;
                    }

                    if (expression.Operand is MemberExpression)
                    {
                        o = $"{o} = '1'";
                    }

                    return $"not ({o})";
                case ExpressionType.Convert:
                    if (expression.Method != null)
                    {
                        return Expression.Lambda(expression).Compile().DynamicInvoke();
                    }
                    break;
            }

            return VisitExpression(expression.Operand);
        }

        /// <summary>
        /// Processes a new expression.
        /// </summary>
        /// <param name="expression">The new expression.</param>
        /// <returns>The result of the processing.</returns>
        protected virtual object VisitNew(NewExpression expression)
        {
            var member = Expression.Convert(expression, typeof(object));
            var lambda = Expression.Lambda<Func<object>>(member);
            var getter = lambda.Compile();
            return getter();
        }

        /// <summary>
        /// Processes a member access expression.
        /// </summary>
        /// <param name="expression">The member access expression.</param>
        /// <returns>The result of the processing.</returns>
        protected virtual object VisitMemberAccess(MemberExpression expression)
        {
            if (expression.Expression?.NodeType == ExpressionType.Parameter)
            {
                return MemberToColumn(expression);
            }

            var member = Expression.Convert(expression, typeof(object));
            var lambda = Expression.Lambda<Func<object>>(member);
            var getter = lambda.Compile();
            return getter();
        }

        /// <summary>
        /// Processes a constant expression.
        /// </summary>
        /// <param name="expression">The constant expression.</param>
        /// <returns>The result of the processing.</returns>
        protected virtual object VisitConstantExpression(ConstantExpression expression) => expression.Value;

        /// <summary>
        /// Proccesses a member expression.
        /// </summary>
        /// <param name="expression">The member expression.</param>
        /// <returns>The result of the processing.</returns>
        protected virtual string MemberToColumn(MemberExpression expression) =>
            $"{DommelMapper.Resolvers.Table(expression.Expression.Type, DommelSqlBuilder)}.{DommelMapper.Resolvers.Column((PropertyInfo)expression.Member, DommelSqlBuilder)}";

        /// <summary>
        /// Returns the expression operant for the specified expression type.
        /// </summary>
        /// <param name="expressionType">The expression type for node of an expression tree.</param>
        /// <returns>The expression operand equivalent of the <paramref name="expressionType"/>.</returns>
        protected virtual string GetOperant(ExpressionType expressionType) => expressionType switch
        {
            ExpressionType.Equal => "=",
            ExpressionType.NotEqual => "<>",
            ExpressionType.GreaterThan => ">",
            ExpressionType.GreaterThanOrEqual => ">=",
            ExpressionType.LessThan => "<",
            ExpressionType.LessThanOrEqual => "<=",
            ExpressionType.AndAlso => "and",
            ExpressionType.OrElse => "or",
            ExpressionType.Add => "+",
            ExpressionType.Subtract => "-",
            ExpressionType.Multiply => "*",
            ExpressionType.Divide => "/",
            ExpressionType.Modulo => "MOD",
            ExpressionType.Coalesce => "COALESCE",
            _ => expressionType.ToString(),
        };

        /// <summary>
        /// Adds a parameter with the specified value to this SQL expression.
        /// </summary>
        /// <param name="value">The value of the parameter.</param>
        /// <param name="paramName">When this method returns, contains the generated parameter name.</param>
        protected virtual void AddParameter(object value, out string paramName)
        {
            _parameterIndex++;
            paramName = DommelSqlBuilder.PrefixParameter($"p{_parameterIndex}");
            Parameters.Add(paramName, value: value);
        }
    }
}
