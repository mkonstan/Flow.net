using CsvHelper;
using CsvHelper.Configuration;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Flow
{
    static class CsvReaderHelpers
    {
        public static CsvReader OpenCsvReader(this string file, CsvConfiguration configuration)
        {

            return new CsvReader(new StreamReader(file), configuration);
        }

        public static CsvWriter OpenCsvWriter(this string file, CsvConfiguration configuration)
        {
            return new CsvWriter(new StreamWriter(file), configuration);
        }

        public static string CsvWriterRecords<T>(this string file, CsvConfiguration configuration, IEnumerable<T> records)
        {
            using (var writer = new CsvWriter(new StreamWriter(file), configuration))
            {
                foreach (var record in records)
                {
                    writer.WriteRecord(record);
                    writer.NextRecord();
                }
            }
            return file;
        }

        public static string[] CsvReaderHeader(this string file, CsvConfiguration configuration)
        {
            using (var reader = file.OpenCsvReader(configuration))
            {
                reader.Read();
                reader.ReadHeader();
                return reader.HeaderRecord;
            }
        }
    }
}
