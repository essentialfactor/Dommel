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

        [Fact]
        public void TranslateSimpleWhere()
        {
            var sql = _sqlQuery
              .Select<Product>(p => new { p.Id, p.Name })
              .Where(x => x.Name == "Test")
              .ToSql(out var parameters);
            var param = parameters.ParameterNames.First();

            Assert.Single(parameters.ParameterNames);
            Assert.Equal("p1", param);
            Assert.Equal("Test", parameters.Get<string>(param));
            AssertQueryMatches("SELECT [Products].[Id], [Products].[Name] FROM [Products] WHERE [Products].[Name] = @p1", sql);
        }

        [Fact]
        public void TranslateWhereClauseForJoined()
        {
            var sql = _sqlQuery
              .InnerJoin<Category>((p, c) => p.CategoryId == c.Id)
              .Select<Product>(p => new { p.Id, p.Name })
              .Where(x => x.Name == "Test")
              .Where<Category>(x => x.Id == 5)
              .ToSql(out var parameters);
            var param = parameters.ParameterNames.First();
            var second = parameters.ParameterNames.Skip(1).First();

            Assert.Equal(2, parameters.ParameterNames.Count());
            Assert.Equal("p1", param);
            Assert.Equal("Test", parameters.Get<string>(param));
            Assert.Equal("p2", second);
            Assert.Equal(5, parameters.Get<int>(second));
            AssertQueryMatches("SELECT [Products].[Id], [Products].[Name] FROM [Products] INNER JOIN [Categories] ON [Products].[CategoryId] = [Categories].[Id] WHERE [Products].[Name] = @p1 AND [Categories].[Id] = @p2", sql);
        }

        [Fact]
        public void ResolveCustomWhereExpression()
        {
            Expression<Func<Product, Category, bool>> selector = (p, c) => p.Name == "Test" && c.Id == 5;

            var sql = _sqlQuery
              .InnerJoin<Category>((p, c) => p.CategoryId == c.Id)
              .Select<Product>(p => new { p.Id, p.Name })
              .Where(selector)
              .ToSql(out var parameters);
            var param = parameters.ParameterNames.First();
            var second = parameters.ParameterNames.Skip(1).First();

            Assert.Equal(2, parameters.ParameterNames.Count());
            Assert.Equal("p1", param);
            Assert.Equal("Test", parameters.Get<string>(param));
            Assert.Equal("p2", second);
            Assert.Equal(5, parameters.Get<int>(second));
            AssertQueryMatches("SELECT [Products].[Id], [Products].[Name] FROM [Products] INNER JOIN [Categories] ON [Products].[CategoryId] = [Categories].[Id] WHERE [Products].[Name] = @p1 AND [Categories].[Id] = @p2", sql);
        }

        // split on and keep the columns in the same order as the entities.

        private void AssertQueryMatches(string expected, string actual)
        {
            var removeLineBreaks = System.Text.RegularExpressions.Regex.Replace(actual, @"\r\n?|\n", " ");
            removeLineBreaks = removeLineBreaks.Replace(" ,", ",");
            Assert.Equal(expected, removeLineBreaks.Trim(), ignoreWhiteSpaceDifferences: true, ignoreCase: true);
        }
    }

}
