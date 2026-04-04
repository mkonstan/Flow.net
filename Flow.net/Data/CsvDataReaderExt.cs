using CsvHelper;
using CsvHelper.Configuration;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;

namespace Flow.Data
{
    public class CsvDataReaderExt : IDataReader
    {
        private readonly CsvReader csv;
        private readonly DataTable schemaTable;
        private bool skipNextRead;

        public CsvDataReaderExt(CsvReader csv, DataTable schemaTable = null)
        {
            this.csv = csv;

            csv.Read();

            if (csv.Configuration.HasHeaderRecord)
            {
                csv.ReadHeader();
            }
            else
            {
                skipNextRead = true;
            }

            this.schemaTable = schemaTable ?? GetSchemaTable();
        }

        public object this[int i]
        {
            get
            {
                return csv[i];
            }
        }

        public object this[string name]
        {
            get
            {
                return csv[name];
            }
        }

        public int Depth
        {
            get
            {
                return 0;
            }
        }

        public bool IsClosed { get; private set; }

        public int RecordsAffected
        {
            get
            {
                return 0;
            }
        }

        public int FieldCount
        {
            get
            {
                return csv?.Parser.Count ?? 0;
            }
        }

        public void Close()
        {
            Dispose();
        }

        public void Dispose()
        {
            csv.Dispose();
            IsClosed = true;
        }

        public bool GetBoolean(int i)
        {
            return csv.GetField<bool>(i);
        }

        public byte GetByte(int i)
        {
            return csv.GetField<byte>(i);
        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            var bytes = csv.GetField<byte[]>(i);

            Array.Copy(bytes, fieldOffset, buffer, bufferoffset, length);

            return bytes.Length;
        }

        public char GetChar(int i)
        {
            return csv.GetField<char>(i);
        }

        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            var chars = csv.GetField(i).ToCharArray();

            Array.Copy(chars, fieldoffset, buffer, bufferoffset, length);

            return chars.Length;
        }

        public IDataReader GetData(int i)
        {
            return null;
        }

        public string GetDataTypeName(int i)
        {
            return typeof(string).Name;
        }

        public DateTime GetDateTime(int i)
        {
            return csv.GetField<DateTime>(i);
        }

        public decimal GetDecimal(int i)
        {
            return csv.GetField<decimal>(i);
        }

        public double GetDouble(int i)
        {
            return csv.GetField<double>(i);
        }

        public Type GetFieldType(int i)
        {
            return typeof(string);
        }

        public float GetFloat(int i)
        {
            return csv.GetField<float>(i);
        }

        public Guid GetGuid(int i)
        {
            return csv.GetField<Guid>(i);
        }

        public short GetInt16(int i)
        {
            return csv.GetField<short>(i);
        }

        public int GetInt32(int i)
        {
            return csv.GetField<int>(i);
        }

        public long GetInt64(int i)
        {
            return csv.GetField<long>(i);
        }

        public string GetName(int i)
        {
            return csv.Configuration.HasHeaderRecord
                ? csv.HeaderRecord[i]
                : string.Empty;
        }

        public int GetOrdinal(string name)
        {
            var index = csv.GetFieldIndex(name, isTryGet: true);
            if (index >= 0)
            {
                return index;
            }

            var args = new PrepareHeaderForMatchArgs(name, 0);
            var namePrepared = csv.Configuration.PrepareHeaderForMatch(args);

            var headerRecord = csv.HeaderRecord;
            for (var i = 0; i < headerRecord.Length; i++)
            {
                args = new PrepareHeaderForMatchArgs(headerRecord[i], i);
                var headerPrepared = csv.Configuration.PrepareHeaderForMatch(args);
                if (csv.Configuration.CultureInfo.CompareInfo.Compare(namePrepared, headerPrepared, CompareOptions.IgnoreCase) == 0)
                {
                    return i;
                }
            }

            throw new IndexOutOfRangeException($"Field with name '{name}' and prepared name '{namePrepared}' was not found.");
        }

        public DataTable GetSchemaTable()
        {
            if (schemaTable != null)
            {
                return schemaTable;
            }

            var dt = new DataTable("SchemaTable");
            dt.Columns.Add("AllowDBNull", typeof(bool));
            dt.Columns.Add("AutoIncrementSeed", typeof(long));
            dt.Columns.Add("AutoIncrementStep", typeof(long));
            dt.Columns.Add("BaseCatalogName");
            dt.Columns.Add("BaseColumnName");
            dt.Columns.Add("BaseColumnNamespace");
            dt.Columns.Add("BaseSchemaName");
            dt.Columns.Add("BaseTableName");
            dt.Columns.Add("BaseTableNamespace");
            dt.Columns.Add("ColumnName");
            dt.Columns.Add("ColumnMapping", typeof(MappingType));
            dt.Columns.Add("ColumnOrdinal", typeof(int));
            dt.Columns.Add("ColumnSize", typeof(int));
            dt.Columns.Add("DataType", typeof(Type));
            dt.Columns.Add("DefaultValue", typeof(object));
            dt.Columns.Add("Expression");
            dt.Columns.Add("IsAutoIncrement", typeof(bool));
            dt.Columns.Add("IsKey", typeof(bool));
            dt.Columns.Add("IsLong", typeof(bool));
            dt.Columns.Add("IsReadOnly", typeof(bool));
            dt.Columns.Add("IsRowVersion", typeof(bool));
            dt.Columns.Add("IsUnique", typeof(bool));
            dt.Columns.Add("NumericPrecision", typeof(short));
            dt.Columns.Add("NumericScale", typeof(short));
            dt.Columns.Add("ProviderType", typeof(int));

            if (csv.Configuration.HasHeaderRecord)
            {
                var header = csv.HeaderRecord;

                for (var i = 0; i < header.Length; i++)
                {
                    var row = dt.NewRow();
                    row["AllowDBNull"] = true;
                    row["AutoIncrementSeed"] = DBNull.Value;
                    row["AutoIncrementStep"] = DBNull.Value;
                    row["BaseCatalogName"] = null;
                    row["BaseColumnName"] = header[i];
                    row["BaseColumnNamespace"] = null;
                    row["BaseSchemaName"] = null;
                    row["BaseTableName"] = null;
                    row["BaseTableNamespace"] = null;
                    row["ColumnName"] = header[i];
                    row["ColumnMapping"] = MappingType.Element;
                    row["ColumnOrdinal"] = i;
                    row["ColumnSize"] = int.MaxValue;
                    row["DataType"] = typeof(string);
                    row["DefaultValue"] = null;
                    row["Expression"] = null;
                    row["IsAutoIncrement"] = false;
                    row["IsKey"] = false;
                    row["IsLong"] = false;
                    row["IsReadOnly"] = true;
                    row["IsRowVersion"] = false;
                    row["IsUnique"] = false;
                    row["NumericPrecision"] = DBNull.Value;
                    row["NumericScale"] = DBNull.Value;
                    row["ProviderType"] = DbType.String;

                    dt.Rows.Add(row);
                }
            }

            return dt;
        }

        public string GetString(int i)
        {
            return csv.GetField(i);
        }

        public object GetValue(int i)
        {
            return IsDBNull(i) ? DBNull.Value : (object)csv.GetField(i);
        }

        public int GetValues(object[] values)
        {
            for (var i = 0; i < values.Length; i++)
            {
                values[i] = IsDBNull(i) ? DBNull.Value : (object)csv.GetField(i);
            }

            return csv.Parser.Count;
        }

        private readonly List<string> NullValues = new List<string>(new[] { string.Empty });

        public bool IsDBNull(int i)
        {
            var field = csv.GetField(i);
            var nullValues = csv.Context.TypeConverterOptionsCache.GetOptions<string>().NullValues;
            if (!nullValues.Any()) nullValues = NullValues;

            return nullValues.Contains(field);
        }

        public bool NextResult()
        {
            return false;
        }

        public bool Read()
        {
            if (skipNextRead)
            {
                skipNextRead = false;
                return true;
            }

            return csv.Read();
        }
    }
}
