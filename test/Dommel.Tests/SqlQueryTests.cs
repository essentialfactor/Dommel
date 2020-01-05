using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using static Dommel.DommelMapper;

namespace Dommel.Tests
{
    public class SqlQueryTests
    {
        private readonly SqlQuery<Product> _sqlQuery = new SqlQuery<Product>(new SqlServerSqlBuilder());
        private readonly SqlQueryTestClass<Product> _sqlQueryTestClass = new SqlQueryTestClass<Product>(new SqlServerSqlBuilder());

        [Fact]
        public void TranslateSelectExpression()
        {
            var sql = _sqlQuery
               .Select<Product>(p => new { p.Id, p.Name })
               .ToSql();
            AssertQueryMatches("SELECT [Products].[Id], [Products].[Name] FROM [Products]", sql);
        }

        [Fact]
        public void MultipleSeparateProperties()
        {
            var sql = _sqlQuery
                .Select<Product>(nameof(Product.Id))
                .Select<Product>(p => new { p.Name })
                .ToSql();
            AssertQueryMatches("SELECT [Products].[Id], [Products].[Name] FROM [Products]", sql);
        }

        [Fact]
        public void CreateDefaultJoin()
        {
            var sql = _sqlQuery
                .Select<Product>(nameof(Product.Id))
                .InnerJoin<Category>()
                .Select<Category>(nameof(Category.Id), nameof(Category.Name))
                .SplitOn(nameof(Category.Id), typeof(Category))
                .ToSql();
            AssertQueryMatches("SELECT [Products].[Id], [Categories].[Id], [Categories].[Name] FROM [Products] INNER JOIN [Categories] ON [Categories].[Id] = [Products].[CategoryId]", sql);
        }

        [Fact]
        public void ColumnNamesFromExpression()
        {
            var sql = _sqlQuery
                .Select<Product>(nameof(Product.Id))
                .InnerJoin<Category>((p, c) => c.Id == p.CategoryId)
                .Select<Category>(nameof(Category.Id), nameof(Category.Name))
                .SplitOn(nameof(Category.Id), typeof(Category))
                .ToSql();
            AssertQueryMatches("SELECT [Products].[Id], [Categories].[Id], [Categories].[Name] FROM [Products] INNER JOIN [Categories] ON [Categories].[Id] = [Products].[CategoryId]", sql);
        }

        // split on and keep the columns in the same order as the entities.

        

        private void AssertQueryMatches(string expected, string actual)
        {
            var removeLineBreaks = System.Text.RegularExpressions.Regex.Replace(actual, @"\r\n?|\n", " ");
            removeLineBreaks = removeLineBreaks.Replace(" ,", ",");
            Assert.Equal(expected, removeLineBreaks.Trim(), ignoreWhiteSpaceDifferences: true);
        }
    }

    public class SqlQueryTestClass<TEntity> : SqlQuery<TEntity> where TEntity : class
    {
        public SqlQueryTestClass(ISqlBuilder sqlBuilder) : base(sqlBuilder)
        {

        }

        public object VisitExpressionTest(Expression expression)
        {
            return VisitExpression(expression);
        }

        public object ResolveJoin<TRelated>(Expression<Func<TEntity, TRelated, bool>> expression)
        {
            return VisitExpression(expression);
        }
    }
}
