using Dapper;
using Microsoft.SqlServer.Server;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;

namespace Flow.Data
{
    public static class QueryHelpers
    {
        public static SqlMapper.ICustomQueryParameter AsTVP<T>(this IEnumerable<T> data, string typeName = null)
            where T : class
        { return new SqlDataRecordParameter<T>(data, typeName); }

        sealed class SqlDataRecordParameter<T> : SqlMapper.ICustomQueryParameter
            where T : class
        {
            private static readonly IDictionary<Type, Func<string, int, SqlMetaData>> TypeMap = GetSqlMetaDataFactory();

            private static readonly SqlDataRecordField[] Fields = GetDataRecordFields();

            private static readonly SqlMetaData[] MetaData = Fields.Select(p => p.GetMetaData()).ToArray();

            private readonly IEnumerable<T> data;
            private readonly string typeName;

            /// <summary>
            /// Create a new instance of SqlDataRecordListTVPParameter
            /// </summary>
            public SqlDataRecordParameter(IEnumerable<T> data, string typeName)
            {
                this.data = data;
                this.typeName = typeName;
            }

            void SqlMapper.ICustomQueryParameter.AddParameter(System.Data.IDbCommand command, string name)
            {
                var param = command.CreateParameter();
                param.ParameterName = name;
                Set(param, data, typeName);
                command.Parameters.Add(param);
            }

            private static void Set(IDbDataParameter parameter, IEnumerable<T> data, string typeName)
            {
                var sqlParam = parameter as System.Data.SqlClient.SqlParameter;
                if (sqlParam != null)
                {
                    parameter.Value = CreateSqlDataRecords(data); //(object)CreateDataReader(data) ?? DBNull.Value;
                    sqlParam.SqlDbType = System.Data.SqlDbType.Structured;
                    sqlParam.TypeName = typeName;
                }
            }

            public static IEnumerable<SqlDataRecord> CreateSqlDataRecords(IEnumerable<T> source)
            {
                foreach (var element in source)
                {
                    var record = new SqlDataRecord(MetaData);
                    record.SetValues(Fields.Select(p => p.GetValue(element)).ToArray());
                    yield return record;
                }
            }

            private static SqlDataRecordField[] GetDataRecordFields()
            {
                return typeof(T)
                .GetProperties(BindingFlags.Instance | BindingFlags.Public)
                    .Where(p => p.CanRead && TypeMap.ContainsKey(p.PropertyType))
                    .Select(p => new SqlDataRecordField(p, TypeMap[p.PropertyType]))
                    .ToArray();
            }

            class SqlDataRecordField
            {
                private readonly Func<string, int, SqlMetaData> _metaDataFactory;
                private readonly PropertyInfo _prop;

                public SqlDataRecordField(PropertyInfo prop, Func<string, int, SqlMetaData> metaDataFactory)
                {
                    _prop = prop;
                    _metaDataFactory = metaDataFactory;
                }

                public string Name => _prop.Name;

                public Type Type => _prop.PropertyType;

                public object GetValue(T @this) { return _prop.GetValue(@this); }

                public SqlMetaData GetMetaData() { return _metaDataFactory(Name, 255); }
            }

            private static IDictionary<Type, Func<string, int, SqlMetaData>> GetSqlMetaDataFactory()
            {
                return new Dictionary<Type, Func<string, int, SqlMetaData>>
                {
                    [typeof(bool)] = (name, size) => new SqlMetaData(name, SqlDbType.Bit),
                    [typeof(bool?)] = (name, size) => new SqlMetaData(name, SqlDbType.Bit),
                    [typeof(byte)] = (name, size) => new SqlMetaData(name, SqlDbType.TinyInt),
                    [typeof(byte?)] = (name, size) => new SqlMetaData(name, SqlDbType.TinyInt),
                    [typeof(byte[])] = (name, size) => new SqlMetaData(name, SqlDbType.VarBinary),
                    [typeof(char)] = (name, size) => new SqlMetaData(name, SqlDbType.NChar, size),
                    [typeof(char?)] = (name, size) => new SqlMetaData(name, SqlDbType.NChar, size),
                    [typeof(DateTime)] = (name, size) => new SqlMetaData(name, SqlDbType.DateTime),
                    [typeof(DateTime?)] = (name, size) => new SqlMetaData(name, SqlDbType.DateTime),
                    [typeof(DateTimeOffset)] = (name, size) => new SqlMetaData(name, SqlDbType.DateTimeOffset),
                    [typeof(DateTimeOffset?)] = (name, size) => new SqlMetaData(name, SqlDbType.DateTimeOffset),
                    [typeof(decimal)] = (name, size) => new SqlMetaData(name, SqlDbType.Money),
                    [typeof(decimal?)] = (name, size) => new SqlMetaData(name, SqlDbType.Money),
                    [typeof(double)] = (name, size) => new SqlMetaData(name, SqlDbType.Float),
                    [typeof(double?)] = (name, size) => new SqlMetaData(name, SqlDbType.Float),
                    [typeof(float)] = (name, size) => new SqlMetaData(name, SqlDbType.Real),
                    [typeof(float?)] = (name, size) => new SqlMetaData(name, SqlDbType.Real),
                    [typeof(Guid)] = (name, size) => new SqlMetaData(name, SqlDbType.UniqueIdentifier),
                    [typeof(Guid?)] = (name, size) => new SqlMetaData(name, SqlDbType.UniqueIdentifier),
                    [typeof(int)] = (name, size) => new SqlMetaData(name, SqlDbType.Int),
                    [typeof(int?)] = (name, size) => new SqlMetaData(name, SqlDbType.Int),
                    [typeof(long)] = (name, size) => new SqlMetaData(name, SqlDbType.BigInt),
                    [typeof(long?)] = (name, size) => new SqlMetaData(name, SqlDbType.BigInt),
                    [typeof(object)] = (name, size) => new SqlMetaData(name, SqlDbType.Variant),
                    [typeof(short)] = (name, size) => new SqlMetaData(name, SqlDbType.SmallInt),
                    [typeof(short?)] = (name, size) => new SqlMetaData(name, SqlDbType.SmallInt),
                    [typeof(string)] = (name, size) => new SqlMetaData(name, SqlDbType.VarChar, size),
                    [typeof(TimeSpan)] = (name, size) => new SqlMetaData(name, SqlDbType.Time),
                    [typeof(TimeSpan?)] = (name, size) => new SqlMetaData(name, SqlDbType.Time)
                };
            }
        }
    }
}
