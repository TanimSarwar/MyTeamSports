using System.Data;

namespace TeamSports.Utilities
{
    public class BasicUtilities
    {
        public static string _connectionString = "";
        public static string GetConnectionString()
        {
            BasicUtilities _basicUtilities = new BasicUtilities();
            var configuation = _basicUtilities.GetConfiguration();
            _connectionString = configuation.GetSection("ConnectionStrings:sqlconnection").Value;
            return _connectionString;
        }
        public IConfigurationRoot GetConfiguration()
        {
            var builder = new ConfigurationBuilder().SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);
            return builder.Build();
        }

        public List<Dictionary<string, object>> GetTableRows(DataTable dtData)
        {
            List<Dictionary<string, object>>
         lstRows = new List<Dictionary<string, object>>();
            Dictionary<string, object> dictRow = null;

            foreach (DataRow dr in dtData.Rows)
            {
                dictRow = new Dictionary<string, object>();
                foreach (DataColumn col in dtData.Columns)
                {
                    dictRow.Add(col.ColumnName, dr[col]);
                }
                lstRows.Add(dictRow);
            }
            return lstRows;
        }

        public List<IList<object>> GetListObject(DataTable dataTable)
        {
            List<IList<object>> data = new List<IList<object>>();

            foreach (DataRow row in dataTable.Rows)
            {
                List<object> rowData = new List<object>();

                foreach (var item in row.ItemArray)
                {
                    rowData.Add(item);
                }

                data.Add(rowData);
            }
            return data;
        }

    }
}
