using Google.Apis.Auth.OAuth2;
using Google.Apis.Script.v1.Data;
using Google.Apis.Services;
using Google.Apis.Sheets.v4;
using Google.Apis.Sheets.v4.Data;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json.Linq;
using System.Data;
using System.Data.Common;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Drawing.Drawing2D;
using System.Drawing.Printing;
using System.Globalization;
using System.Text.RegularExpressions;
using TeamSports.DAL;
using TeamSports.Models;
using TeamSports.Utilities;

namespace TeamSports.Controllers
{
    public class HomeController : Controller
    {
        private readonly ILogger<HomeController> _logger;
        private readonly IHttpClientFactory _httpClientFactory;

        TeamDAL _dal = new TeamDAL();
        BasicUtilities _basicUtilities = new BasicUtilities();
        public HomeController(ILogger<HomeController> logger)
        {
            _logger = logger;
            // _httpClientFactory = httpClientFactory ?? throw new ArgumentNullException(nameof(httpClientFactory));
        }



        public IActionResult Index()
        {
            return View();
        }
        [Route("brand-file-upload")]
        public IActionResult BRAND_FILE_UPLOAD()
        {
            return View();
        }
        [Route("scraper-file-upload")]
        public IActionResult SCRAP_FILE_UPLOAD()
        {
            return View();
        }
        [Route("single-entry")]
        public IActionResult SINGLE_ARTICLE_UPLOAD()
        {
            return View();
        }

        [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
        public IActionResult Error()
        {
            return View(new ErrorViewModel { RequestId = Activity.Current?.Id ?? HttpContext.TraceIdentifier });
        }

        [HttpPost]
        [DisableRequestSizeLimit]
        public async Task<JsonResult> ImportExcelFile(IFormFile excelFile, string vBrandID, string vBrandName, string vFileType)
        {
            /*
             vFileType = 1 = Brand File
                         2 = Scrapper File
             
             */
            try
            {
                vBrandName = vBrandName.Trim();

                var MainPath = Path.Combine(Directory.GetCurrentDirectory(), "wwwroot", "Uploads");


                if (!Directory.Exists(MainPath))
                {
                    Directory.CreateDirectory(MainPath);
                }

                //get file path 
                var filePath = Path.Combine(MainPath, excelFile.FileName);
                using (System.IO.Stream stream = new FileStream(filePath, FileMode.Create))
                {
                    await excelFile.CopyToAsync(stream);
                }

                //get extension
                string extension = Path.GetExtension(excelFile.FileName);
                string conString = string.Empty;

                switch (extension)
                {
                    case ".xls": //Excel 97-03.
                        conString = "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=" + filePath + ";Extended Properties='Excel 8.0;HDR=YES'";
                        break;
                    case ".xlsx": //Excel 07 and above.
                        conString = "Provider=Microsoft.ACE.OLEDB.12.0;Data Source=" + filePath + ";Extended Properties='Excel 8.0;HDR=YES'";
                        break;
                    case ".csv": //csv.
                        conString = $"Provider=Microsoft.ACE.OLEDB.12.0;Data Source={System.IO.Path.GetDirectoryName(filePath)};Extended Properties='text;HDR=Yes;'";
                        break;
                }



                DataTable dt = new DataTable();


                if (extension.ToLower() == ".csv")
                {

                    using (OleDbConnection connection = new OleDbConnection(conString))
                    {
                        // Open the connection
                        connection.Open();

                        // Select all data from the CSV file
                        string query = $"SELECT * FROM [{System.IO.Path.GetFileName(filePath)}]";

                        using (OleDbCommand command = new OleDbCommand(query, connection))
                        {
                            using (OleDbDataAdapter adapter = new OleDbDataAdapter(command))
                            {
                                adapter.Fill(dt);

                            }
                        }
                    }
                }
                else
                {

                    conString = string.Format(conString, filePath);
                    using (OleDbConnection connExcel = new OleDbConnection(conString))
                    {
                        using (OleDbCommand cmdExcel = new OleDbCommand())
                        {
                            using (OleDbDataAdapter odaExcel = new OleDbDataAdapter())
                            {
                                cmdExcel.Connection = connExcel;
                                connExcel.Open();
                                DataTable dtExcelSchema;
                                dtExcelSchema = connExcel.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, null);
                                string sheetName = dtExcelSchema.Rows[0]["TABLE_NAME"].ToString();
                                cmdExcel.CommandText = "SELECT * From [" + sheetName + "]";
                                odaExcel.SelectCommand = cmdExcel;
                                odaExcel.Fill(dt);
                                connExcel.Close();
                            }
                        }
                    }
                }
















                if (dt.Rows.Count > 0)
                {


                    var cleanedDataTable = new DataTable();
                    var FinalcleanedDataTable = new DataTable();

                    foreach (DataColumn column in dt.Columns)
                    {
                        cleanedDataTable.Columns.Add(column.ColumnName, column.DataType);
                        FinalcleanedDataTable.Columns.Add(column.ColumnName?.Replace(" ", ""), column.DataType);
                    }
                    foreach (DataRow row in dt.Rows)
                    {
                        var newRow = FinalcleanedDataTable.NewRow();
                        foreach (DataColumn column in cleanedDataTable.Columns)
                        {
                            var value = row[column.ColumnName];
                            if (value != null && value != DBNull.Value && value is string)
                            {
                                newRow[column.ColumnName?.Replace(" ", "")] = ((string)value).Trim();
                            }
                            else
                            {
                                newRow[column.ColumnName?.Replace(" ", "")] = value;
                            }
                        }
                        FinalcleanedDataTable.Rows.Add(newRow);
                    }




                    bool result = await ExcelDataProcess(vBrandID, vBrandName, vFileType, FinalcleanedDataTable);
                    if (result)
                    {
                        return Json(result);
                    }

                    return Json(false);
                }

                return Json(false);

            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                return Json(false);
            }
        }
        [HttpPost]
        public JsonResult GetDD_DATA(string _type)
        {
            DataTable dt = new DataTable();
            dt = _dal.GETDD_DATA(_type);
            List<Dictionary<string, object>> _list = _basicUtilities.GetTableRows(dt);
            return Json(_list);
        }
        private async Task<bool> ExcelDataProcess(string vBrandID, string vBrandName, string vFileType, DataTable dt)
        {
            try
            {

                var config = _basicUtilities.GetConfiguration();

                string conString = config.GetSection("ConnectionStrings:sqlconnection").Value;
                DataTable FinalData = new DataTable();

                if (vBrandName == "PUMA" && vFileType == "Brand")
                {
                    FinalData = ProcessPumaBrandFile(vBrandID, vBrandName, dt);
                }
                else if (vBrandName == "JAKO" && vFileType == "Brand")
                {
                    FinalData = ProcessJakoBrandFile(vBrandID, vBrandName, dt);
                }
                else if (vBrandName == "ERIMA" && vFileType == "Brand")
                {
                    FinalData = ProcessErimaBrandFile(vBrandID, vBrandName, dt);
                }
                else if (vBrandName == "HUMMEL" && vFileType == "Brand")
                {
                    FinalData = ProcessHummelBrandFile(vBrandID, vBrandName, dt);
                }
                else if (vFileType == "Scraper")
                {
                    FinalData = ProcessScraperFile(vBrandID, vBrandName, dt);
                }
                if (FinalData.Rows.Count > 0)
                {

                    string FinalTable = "MAIN_SHEET_TMP";
                    int i = _dal.DeleteBrandFiles(FinalTable, vBrandID);
                    using (SqlConnection con = new SqlConnection(conString))
                    {
                        using (SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(con))
                        {
                            sqlBulkCopy.BulkCopyTimeout = 600;
                            sqlBulkCopy.DestinationTableName = "dbo." + FinalTable;
                            DataColumnCollection dataColumnCollection = FinalData.Columns;
                            for (int j = 0; j < dataColumnCollection.Count; j++)
                            {
                                string columnName = dataColumnCollection[j].ToString();
                                sqlBulkCopy.ColumnMappings.Add(columnName, columnName);
                            }
                            con.Open();
                            sqlBulkCopy.WriteToServer(FinalData);
                            con.Close();
                        }
                    }

                    //Push Data to google sheet
                    //DataTable newData = FinalData;
                    //newData.Columns.Remove("EAN");
                    //newData.Columns.Remove("BRANDID");
                    //bool output = await UploadDataToSheet(newData, vBrandName);


                    return true;
                }


                return false;




            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                return false;
            }

        }


        private DataTable ProcessScraperFile(string vBrandID, string vBrandName, DataTable dt)
        {
            DataTable resultTable = new DataTable();
            DataTable dataTable = new DataTable();



            DataColumnCollection columns = dt.Columns;
            if (!columns.Contains("Price"))
            {
                dt.Columns.Add("Price");
            }






            try
            {
                resultTable.Columns.Add("BRANDID");
                resultTable.Columns.Add("BRAND");
                resultTable.Columns.Add("LINE");
                resultTable.Columns.Add("PROD_NAME");
                resultTable.Columns.Add("PROD_NUMBER");
                resultTable.Columns.Add("UNIFYING_PROD_ID");
                resultTable.Columns.Add("SEPERATING_PROD_ID");
                resultTable.Columns.Add("TITLE");
                resultTable.Columns.Add("PRODUCT_TYPE");
                resultTable.Columns.Add("PROD_GENDER");
                resultTable.Columns.Add("EAN");
                resultTable.Columns.Add("PROD_DESCRIPTION");
                resultTable.Columns.Add("HTML_BODY");
                resultTable.Columns.Add("VENDOR");
                resultTable.Columns.Add("TAGS");
                resultTable.Columns.Add("PUBLISHED");
                resultTable.Columns.Add("MANUFACTURER_SIZE_SPECTRUM");
                resultTable.Columns.Add("STORE_SIZE_SPECTRUM");
                resultTable.Columns.Add("MANUFAC_COLOR_SPECTRUM_NAMES");
                resultTable.Columns.Add("MANUFAC_COLOR_SPECTRUM_CODES");
                resultTable.Columns.Add("STORE_COLOR_SPECTRUM");
                resultTable.Columns.Add("COLOR_SELECTION");
                resultTable.Columns.Add("EXTRA_OPT_NAME");
                resultTable.Columns.Add("EXTRA_OPT_VAL");
                resultTable.Columns.Add("VERSION_NAME");
                resultTable.Columns.Add("BASE_PRICE");
                resultTable.Columns.Add("VARIANT_GRAMS");
                resultTable.Columns.Add("VARIANT_INV_TRACKER");
                resultTable.Columns.Add("VARIANT_INV_QTY");
                resultTable.Columns.Add("VARIANT_INV_POLICY");
                resultTable.Columns.Add("VARIANT_FULFILLMENT_SERVICE");
                resultTable.Columns.Add("VARIANT_COMP_AT_PRICE");
                resultTable.Columns.Add("VARIANT_REQ_SHIPPING");
                resultTable.Columns.Add("VAR_TAXABLE");
                resultTable.Columns.Add("VARIANT_BCODE");
                resultTable.Columns.Add("IMAGE_POSITION");
                resultTable.Columns.Add("IMAGE_ALT_TXT");
                resultTable.Columns.Add("GIFT_CARD");
                resultTable.Columns.Add("SEO_TITLE");
                resultTable.Columns.Add("VARIANT_IMAGE");
                resultTable.Columns.Add("VARIANT_WEIGHT_UNIT");
                resultTable.Columns.Add("VARIANT_TAX_CODE");
                resultTable.Columns.Add("COST_PER_ITEM");
                resultTable.Columns.Add("PRICE_INTERNATIONAL");
                resultTable.Columns.Add("COMP_AT_PRICE_INTL");
                resultTable.Columns.Add("STATUS");
                resultTable.Columns.Add("PROD_FILE_NAME");
                resultTable.Columns.Add("COLOR_NAMES");

                //dataTable = dt.AsEnumerable()
                //     .OrderBy(row => row.Field<string>("ProductNumber"))
                //     .ThenBy(row => row.Field<string>("Size"))
                //     .CopyToDataTable();

                dataTable = SortDataTable(dt, "ProductNumber", "Size", "Color");

                if (vBrandName.ToLower() == "nike")
                {
                    dataTable.Columns.Add("colorcode");
                    foreach (DataRow r in dataTable.Rows)
                    {
                       string colorcode= vBrandName.ToLower() == "nike" && r["ProductNumber"].ToString().Trim().Contains('-') ? r["ProductNumber"].ToString().Trim().Split('-')[1] : "0";
                        string productname = vBrandName.ToLower() == "nike" && r["ProductNumber"].ToString().Trim().Contains('-') ? r["ProductNumber"].ToString().Trim().Split('-')[0] : r["ProductNumber"].ToString().Trim();
                        r["ProductNumber"] = productname;
                        r["colorcode"] = colorcode;


                    }
                    var groupedData = from row in dataTable.AsEnumerable()
                                      group row by new { PROD_NUMBER = row["ProductNumber"], BASE_PRICE = row["Price"], GENDER = GenderMapping(row["Gender"].ToString()) } into grp
                                      select new
                                      {
                                          PROD_NAME = string.Join("#", grp.Select(r => r["Name"]).Distinct()),
                                          DESCRIPTION = string.Join("#", grp.Select(r => r["Desccription"]).Distinct()),
                                          PROD_NUMBER = grp.Key.PROD_NUMBER,
                                          GENDER = grp.Key.GENDER,
                                          BASE_PRICE = grp.Key.BASE_PRICE,
                                          EAN = string.Join(";", grp.Select(r => r["EAN"]).Distinct()),
                                          SIZE = string.Join(";", grp.Select(r => r["Size"]).Distinct()),
                                          COLORCODE = string.Join(";", grp.Select(r => r["colorcode"]).Distinct()),
                                          COLORNAME = string.Join(";", grp.Select(r => r["Color"]).Distinct()),
                                          Images = string.Join(";", grp.Select(r => r["Images"]).Distinct())
                                      };




                    string TableName = "EAN_DB";

                    // Delete any existing data regarding selected brand
                    int i = _dal.DeleteBrandFiles(TableName, vBrandID);


                    DataTable EAN_DB_DATA = new DataTable();
                    EAN_DB_DATA.Columns.Add("BRAND_ID");
                    EAN_DB_DATA.Columns.Add("BRAND_NAME");
                    EAN_DB_DATA.Columns.Add("PRODUCT_NAME");
                    EAN_DB_DATA.Columns.Add("PRODUCT_NUMBER");
                    EAN_DB_DATA.Columns.Add("PRODUCT_GENDER");
                    EAN_DB_DATA.Columns.Add("PRICE_UVP");
                    EAN_DB_DATA.Columns.Add("EAN");
                    EAN_DB_DATA.Columns.Add("SIZE");
                    EAN_DB_DATA.Columns.Add("COLOR_CODE");
                    EAN_DB_DATA.Columns.Add("COLOR_NAME");
                    EAN_DB_DATA.Columns.Add("IMAGE_URL"); EAN_DB_DATA.Columns.Add("STATUS");
                    string ProdName = string.Empty;
                    string Prodnumber = string.Empty;

                    foreach (DataRow row in dataTable.Rows)
                    {
                        string PROD_NUMBER = row["ProductNumber"].ToString().Trim();

                        if (row["Price"].ToString().Trim() == "" || row["Price"].ToString().Trim() == "0") continue;
                        if (Prodnumber != PROD_NUMBER)
                        {
                            ProdName = row["Name"].ToString();
                            Prodnumber = PROD_NUMBER;
                        }



                        var newRow = EAN_DB_DATA.NewRow();
                        newRow["BRAND_ID"] = vBrandID;
                        newRow["BRAND_NAME"] = vBrandName;
                        newRow["PRODUCT_NAME"] = replaceGermanUmlauts(ProdName);

                        string ColorCode = row["colorcode"].ToString().Trim();
                        newRow["PRODUCT_NUMBER"] = PROD_NUMBER;
                        newRow["PRODUCT_GENDER"] = GenderMapping(row["Gender"].ToString());
                        newRow["PRICE_UVP"] = row["Price"].ToString().Trim()?.Replace('.', ',');
                        newRow["EAN"] = row["EAN"];
                        newRow["SIZE"] = row["Size"];
                        newRow["COLOR_CODE"] = ColorCode;
                        newRow["COLOR_NAME"] = replaceGermanUmlauts(row["Color"].ToString());
                        newRow["IMAGE_URL"] = row["Images"];
                        newRow["STATUS"] = 0;
                        EAN_DB_DATA.Rows.Add(newRow);
                    }



                    var config = _basicUtilities.GetConfiguration();
                    string conString = config.GetSection("ConnectionStrings:sqlconnection").Value;
                    using (SqlConnection con = new SqlConnection(conString))
                    {

                        using (SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(con))
                        {

                            sqlBulkCopy.BulkCopyTimeout = 600;
                            sqlBulkCopy.DestinationTableName = "dbo." + TableName;
                            DataColumnCollection dataColumnCollection = EAN_DB_DATA.Columns;
                            for (int j = 0; j < dataColumnCollection.Count; j++)
                            {
                                string columnName = dataColumnCollection[j].ToString()?.Replace(" ", "");
                                sqlBulkCopy.ColumnMappings.Add(columnName, columnName);
                            }
                            con.Open();
                            sqlBulkCopy.WriteToServer(EAN_DB_DATA);
                            con.Close();
                        }
                    }






                    string expression = "<.*?>";

                    foreach (var item in groupedData)
                    {
                        if (item.BASE_PRICE.ToString().Trim() == "0" || item.BASE_PRICE.ToString().Trim() == "") continue;

                        DataRow newRow = resultTable.NewRow();

                        newRow["BRANDID"] = vBrandID;
                        newRow["EAN"] = item.EAN.ToString().Trim();
                        newRow["BRAND"] = vBrandName;
                        newRow["LINE"] = "".ToString().Trim();
                        newRow["PROD_NAME"] = item.PROD_NAME.Split("#")[0].ToString().Trim();

                        string PROD_NUMBER = item.PROD_NUMBER.ToString().Trim();
                        string COLORCODE = item.COLORCODE.ToString().Trim();

                        newRow["PROD_NUMBER"] = PROD_NUMBER;
                        newRow["UNIFYING_PROD_ID"] = PROD_NUMBER;


                        string gender = GenderMapping(item.GENDER.ToString());
                        newRow["SEPERATING_PROD_ID"] = gender.Trim().Length > 0 ? PROD_NUMBER + " - " + gender.Trim() : PROD_NUMBER;

                        newRow["TITLE"] = item.PROD_NAME.Split("#")[0].ToString().Trim();
                        newRow["PRODUCT_TYPE"] = "".ToString().Trim();
                        newRow["PROD_GENDER"] = gender.Trim();
                        newRow["PROD_DESCRIPTION"] = replaceGermanUmlauts(Regex.Replace(item.DESCRIPTION.Split("#")[0].ToString().Trim(), expression, " ").Trim());
                        newRow["HTML_BODY"] = "".ToString().Trim();
                        newRow["VENDOR"] = "".ToString().Trim();
                        newRow["TAGS"] = "".ToString().Trim();
                        newRow["PUBLISHED"] = "".ToString().Trim();
                        newRow["MANUFACTURER_SIZE_SPECTRUM"] = item.SIZE.ToString().Trim();
                        newRow["STORE_SIZE_SPECTRUM"] = item.SIZE.ToString().Trim();
                        newRow["MANUFAC_COLOR_SPECTRUM_NAMES"] = replaceGermanUmlauts(item.COLORNAME.ToString().Trim());
                        newRow["MANUFAC_COLOR_SPECTRUM_CODES"] = COLORCODE;
                        newRow["STORE_COLOR_SPECTRUM"] = replaceGermanUmlauts(item.COLORNAME.ToString().Trim());
                        newRow["COLOR_SELECTION"] = "".ToString().Trim();
                        newRow["EXTRA_OPT_NAME"] = "".ToString().Trim();
                        newRow["EXTRA_OPT_VAL"] = "".ToString().Trim();
                        newRow["VERSION_NAME"] = "".ToString().Trim();
                        newRow["BASE_PRICE"] = item.BASE_PRICE.ToString().Trim()?.Replace('.', ',');
                        newRow["VARIANT_GRAMS"] = "".ToString().Trim();
                        newRow["VARIANT_INV_TRACKER"] = "".ToString().Trim();
                        newRow["VARIANT_INV_QTY"] = "".ToString().Trim();
                        newRow["VARIANT_INV_POLICY"] = "".ToString().Trim();
                        newRow["VARIANT_FULFILLMENT_SERVICE"] = "".ToString().Trim();
                        newRow["VARIANT_COMP_AT_PRICE"] = "".ToString().Trim();
                        newRow["VARIANT_REQ_SHIPPING"] = "".ToString().Trim();
                        newRow["VAR_TAXABLE"] = "".ToString().Trim();
                        newRow["VARIANT_BCODE"] = "".ToString().Trim();
                        newRow["IMAGE_POSITION"] = "".ToString().Trim();
                        newRow["IMAGE_ALT_TXT"] = "".ToString().Trim();
                        newRow["GIFT_CARD"] = "".ToString().Trim();
                        newRow["SEO_TITLE"] = "".ToString().Trim();
                        newRow["VARIANT_IMAGE"] = item.Images.ToString().Trim();
                        newRow["VARIANT_WEIGHT_UNIT"] = "".ToString().Trim();
                        newRow["VARIANT_TAX_CODE"] = "".ToString().Trim();
                        newRow["COST_PER_ITEM"] = "".ToString().Trim();
                        newRow["PRICE_INTERNATIONAL"] = "".ToString().Trim();
                        newRow["COMP_AT_PRICE_INTL"] = "".ToString().Trim();
                        newRow["STATUS"] = "".ToString().Trim();

                        string inputString = item.PROD_NAME.ToString().Trim().ToLower();
                        string stringWithHyphens = Regex.Replace(inputString, @"\s", "-");
                        string result = Regex.Replace(stringWithHyphens, @"[^\w\d;]", "");
                        result = replaceGermanUmlauts(result);
                        newRow["PROD_FILE_NAME"] = result;

                        inputString = item.COLORNAME.ToString().Trim().ToLower();
                        stringWithHyphens = Regex.Replace(inputString, @"\s", "-");
                        result = Regex.Replace(stringWithHyphens, @"[^\w\d;]", "");

                        newRow["COLOR_NAMES"] = ";" + replaceGermanUmlauts(result);
                        resultTable.Rows.Add(newRow);
                    }
                }
                else
                {
                    var groupedData = from row in dataTable.AsEnumerable()
                                      group row by new { PROD_NUMBER = row["ProductNumber"], BASE_PRICE = row["Price"], GENDER = GenderMapping(row["Gender"].ToString()) } into grp
                                      select new
                                      {
                                          PROD_NAME = string.Join("#", grp.Select(r => r["Name"]).Distinct()),
                                          DESCRIPTION = string.Join("#", grp.Select(r => r["Desccription"]).Distinct()),
                                          PROD_NUMBER = grp.Key.PROD_NUMBER,
                                          GENDER = grp.Key.GENDER,
                                          BASE_PRICE = grp.Key.BASE_PRICE,
                                          EAN = string.Join(";", grp.Select(r => r["EAN"]).Distinct()),
                                          SIZE = string.Join(";", grp.Select(r => r["Size"]).Distinct()),
                                          COLORNAME = string.Join(";", grp.Select(r => r["Color"]).Distinct()),
                                          Images = string.Join(";", grp.Select(r => r["Images"]).Distinct())
                                      };




                    string TableName = "EAN_DB";

                    // Delete any existing data regarding selected brand
                    int i = _dal.DeleteBrandFiles(TableName, vBrandID);


                    DataTable EAN_DB_DATA = new DataTable();
                    EAN_DB_DATA.Columns.Add("BRAND_ID");
                    EAN_DB_DATA.Columns.Add("BRAND_NAME");
                    EAN_DB_DATA.Columns.Add("PRODUCT_NAME");
                    EAN_DB_DATA.Columns.Add("PRODUCT_NUMBER");
                    EAN_DB_DATA.Columns.Add("PRODUCT_GENDER");
                    EAN_DB_DATA.Columns.Add("PRICE_UVP");
                    EAN_DB_DATA.Columns.Add("EAN");
                    EAN_DB_DATA.Columns.Add("SIZE");
                    EAN_DB_DATA.Columns.Add("COLOR_CODE");
                    EAN_DB_DATA.Columns.Add("COLOR_NAME");
                    EAN_DB_DATA.Columns.Add("IMAGE_URL"); EAN_DB_DATA.Columns.Add("STATUS");
                    string ProdName = string.Empty;
                    string Prodnumber = string.Empty;

                    foreach (DataRow row in dataTable.Rows)
                    {
                        string PROD_NUMBER = row["ProductNumber"].ToString().Trim();

                        if (row["Price"].ToString().Trim() == "" || row["Price"].ToString().Trim() == "0") continue;
                        if (Prodnumber != PROD_NUMBER)
                        {
                            ProdName = row["Name"].ToString();
                            Prodnumber = PROD_NUMBER;
                        }



                        var newRow = EAN_DB_DATA.NewRow();
                        newRow["BRAND_ID"] = vBrandID;
                        newRow["BRAND_NAME"] = vBrandName;
                        newRow["PRODUCT_NAME"] = replaceGermanUmlauts(ProdName);

                        string ColorCode = "0";
                        newRow["PRODUCT_NUMBER"] = PROD_NUMBER;
                        newRow["PRODUCT_GENDER"] = GenderMapping(row["Gender"].ToString());
                        newRow["PRICE_UVP"] = row["Price"].ToString().Trim()?.Replace('.', ',');
                        newRow["EAN"] = row["EAN"];
                        newRow["SIZE"] = row["Size"];
                        newRow["COLOR_CODE"] = ColorCode;
                        newRow["COLOR_NAME"] = replaceGermanUmlauts(row["Color"].ToString());
                        newRow["IMAGE_URL"] = row["Images"];
                        newRow["STATUS"] = 0;
                        EAN_DB_DATA.Rows.Add(newRow);
                    }



                    var config = _basicUtilities.GetConfiguration();
                    string conString = config.GetSection("ConnectionStrings:sqlconnection").Value;
                    using (SqlConnection con = new SqlConnection(conString))
                    {

                        using (SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(con))
                        {

                            sqlBulkCopy.BulkCopyTimeout = 600;
                            sqlBulkCopy.DestinationTableName = "dbo." + TableName;
                            DataColumnCollection dataColumnCollection = EAN_DB_DATA.Columns;
                            for (int j = 0; j < dataColumnCollection.Count; j++)
                            {
                                string columnName = dataColumnCollection[j].ToString()?.Replace(" ", "");
                                sqlBulkCopy.ColumnMappings.Add(columnName, columnName);
                            }
                            con.Open();
                            sqlBulkCopy.WriteToServer(EAN_DB_DATA);
                            con.Close();
                        }
                    }






                    string expression = "<.*?>";

                    foreach (var item in groupedData)
                    {
                        if (item.BASE_PRICE.ToString().Trim() == "0" || item.BASE_PRICE.ToString().Trim() == "") continue;

                        DataRow newRow = resultTable.NewRow();

                        newRow["BRANDID"] = vBrandID;
                        newRow["EAN"] = item.EAN.ToString().Trim();
                        newRow["BRAND"] = vBrandName;
                        newRow["LINE"] = "".ToString().Trim();
                        newRow["PROD_NAME"] = item.PROD_NAME.Split("#")[0].ToString().Trim();

                        string PROD_NUMBER = item.PROD_NUMBER.ToString().Trim();
                        string colorcode = "0".ToString().Trim();

                        newRow["PROD_NUMBER"] = PROD_NUMBER;
                        newRow["UNIFYING_PROD_ID"] = PROD_NUMBER;


                        string gender = GenderMapping(item.GENDER.ToString());
                        newRow["SEPERATING_PROD_ID"] = gender.Trim().Length > 0 ? PROD_NUMBER + " - " + gender.Trim() : PROD_NUMBER;

                        newRow["TITLE"] = item.PROD_NAME.Split("#")[0].ToString().Trim();
                        newRow["PRODUCT_TYPE"] = "".ToString().Trim();
                        newRow["PROD_GENDER"] = gender.Trim();
                        newRow["PROD_DESCRIPTION"] = replaceGermanUmlauts(Regex.Replace(item.DESCRIPTION.Split("#")[0].ToString().Trim(), expression, " ").Trim());
                        newRow["HTML_BODY"] = "".ToString().Trim();
                        newRow["VENDOR"] = "".ToString().Trim();
                        newRow["TAGS"] = "".ToString().Trim();
                        newRow["PUBLISHED"] = "".ToString().Trim();
                        newRow["MANUFACTURER_SIZE_SPECTRUM"] = item.SIZE.ToString().Trim();
                        newRow["STORE_SIZE_SPECTRUM"] = item.SIZE.ToString().Trim();
                        newRow["MANUFAC_COLOR_SPECTRUM_NAMES"] = replaceGermanUmlauts(item.COLORNAME.ToString().Trim());
                        newRow["MANUFAC_COLOR_SPECTRUM_CODES"] = colorcode;
                        newRow["STORE_COLOR_SPECTRUM"] = item.COLORNAME.ToString().Trim();
                        newRow["COLOR_SELECTION"] = "".ToString().Trim();
                        newRow["EXTRA_OPT_NAME"] = "".ToString().Trim();
                        newRow["EXTRA_OPT_VAL"] = "".ToString().Trim();
                        newRow["VERSION_NAME"] = "".ToString().Trim();
                        newRow["BASE_PRICE"] = item.BASE_PRICE.ToString().Trim()?.Replace('.', ',');
                        newRow["VARIANT_GRAMS"] = "".ToString().Trim();
                        newRow["VARIANT_INV_TRACKER"] = "".ToString().Trim();
                        newRow["VARIANT_INV_QTY"] = "".ToString().Trim();
                        newRow["VARIANT_INV_POLICY"] = "".ToString().Trim();
                        newRow["VARIANT_FULFILLMENT_SERVICE"] = "".ToString().Trim();
                        newRow["VARIANT_COMP_AT_PRICE"] = "".ToString().Trim();
                        newRow["VARIANT_REQ_SHIPPING"] = "".ToString().Trim();
                        newRow["VAR_TAXABLE"] = "".ToString().Trim();
                        newRow["VARIANT_BCODE"] = "".ToString().Trim();
                        newRow["IMAGE_POSITION"] = "".ToString().Trim();
                        newRow["IMAGE_ALT_TXT"] = "".ToString().Trim();
                        newRow["GIFT_CARD"] = "".ToString().Trim();
                        newRow["SEO_TITLE"] = "".ToString().Trim();
                        newRow["VARIANT_IMAGE"] = item.Images.ToString().Trim();
                        newRow["VARIANT_WEIGHT_UNIT"] = "".ToString().Trim();
                        newRow["VARIANT_TAX_CODE"] = "".ToString().Trim();
                        newRow["COST_PER_ITEM"] = "".ToString().Trim();
                        newRow["PRICE_INTERNATIONAL"] = "".ToString().Trim();
                        newRow["COMP_AT_PRICE_INTL"] = "".ToString().Trim();
                        newRow["STATUS"] = "".ToString().Trim();

                        string inputString = item.PROD_NAME.ToString().Trim().ToLower();
                        string stringWithHyphens = Regex.Replace(inputString, @"\s", "-");
                        string result = Regex.Replace(stringWithHyphens, @"[^\w\d;]", "");
                        result = replaceGermanUmlauts(result);
                        newRow["PROD_FILE_NAME"] = result;

                        inputString = item.COLORNAME.ToString().Trim().ToLower();
                        stringWithHyphens = Regex.Replace(inputString, @"\s", "-");
                        result = Regex.Replace(stringWithHyphens, @"[^\w\d;]", "");

                        newRow["COLOR_NAMES"] = ";" + replaceGermanUmlauts(result);
                        resultTable.Rows.Add(newRow);
                    }
                }


               
                dataTable = resultTable;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                dataTable = new DataTable();
            }
            return dataTable;

        }
        private DataTable ProcessHummelBrandFile(string vBrandID, string vBrandName, DataTable dt)
        {
            DataTable resultTable = new DataTable();
            DataTable dataTable = new DataTable();
            try
            {
                resultTable.Columns.Add("BRANDID");
                resultTable.Columns.Add("BRAND");
                resultTable.Columns.Add("LINE");
                resultTable.Columns.Add("PROD_NAME");
                resultTable.Columns.Add("PROD_NUMBER");
                resultTable.Columns.Add("UNIFYING_PROD_ID");
                resultTable.Columns.Add("SEPERATING_PROD_ID");
                resultTable.Columns.Add("TITLE");
                resultTable.Columns.Add("PRODUCT_TYPE");
                resultTable.Columns.Add("PROD_GENDER");
                resultTable.Columns.Add("EAN");
                resultTable.Columns.Add("PROD_DESCRIPTION");
                resultTable.Columns.Add("HTML_BODY");
                resultTable.Columns.Add("VENDOR");
                resultTable.Columns.Add("TAGS");
                resultTable.Columns.Add("PUBLISHED");
                resultTable.Columns.Add("MANUFACTURER_SIZE_SPECTRUM");
                resultTable.Columns.Add("STORE_SIZE_SPECTRUM");
                resultTable.Columns.Add("MANUFAC_COLOR_SPECTRUM_NAMES");
                resultTable.Columns.Add("MANUFAC_COLOR_SPECTRUM_CODES");
                resultTable.Columns.Add("STORE_COLOR_SPECTRUM");
                resultTable.Columns.Add("COLOR_SELECTION");
                resultTable.Columns.Add("EXTRA_OPT_NAME");
                resultTable.Columns.Add("EXTRA_OPT_VAL");
                resultTable.Columns.Add("VERSION_NAME");
                resultTable.Columns.Add("BASE_PRICE");
                resultTable.Columns.Add("VARIANT_GRAMS");
                resultTable.Columns.Add("VARIANT_INV_TRACKER");
                resultTable.Columns.Add("VARIANT_INV_QTY");
                resultTable.Columns.Add("VARIANT_INV_POLICY");
                resultTable.Columns.Add("VARIANT_FULFILLMENT_SERVICE");
                resultTable.Columns.Add("VARIANT_COMP_AT_PRICE");
                resultTable.Columns.Add("VARIANT_REQ_SHIPPING");
                resultTable.Columns.Add("VAR_TAXABLE");
                resultTable.Columns.Add("VARIANT_BCODE");
                resultTable.Columns.Add("IMAGE_POSITION");
                resultTable.Columns.Add("IMAGE_ALT_TXT");
                resultTable.Columns.Add("GIFT_CARD");
                resultTable.Columns.Add("SEO_TITLE");
                resultTable.Columns.Add("VARIANT_IMAGE");
                resultTable.Columns.Add("VARIANT_WEIGHT_UNIT");
                resultTable.Columns.Add("VARIANT_TAX_CODE");
                resultTable.Columns.Add("COST_PER_ITEM");
                resultTable.Columns.Add("PRICE_INTERNATIONAL");
                resultTable.Columns.Add("COMP_AT_PRICE_INTL");
                resultTable.Columns.Add("STATUS");
                resultTable.Columns.Add("PROD_FILE_NAME");
                resultTable.Columns.Add("COLOR_NAMES");

                //dataTable = dt.AsEnumerable()
                //   .OrderBy(row => row.Field<string>("StyleNo"))

                //   .ThenBy(row => GetSortValue(row.Field<object>("Größe ")))
                //   .CopyToDataTable();


                dataTable = SortDataTable(dt, "StyleNo", "Größe", "lookupColorName");

                var groupedData = from row in dataTable.AsEnumerable()
                                  group row by new { PROD_NUMBER = row["StyleNo"], BASE_PRICE = row["LISTPRICEDEEUR"], GENDER = GenderMapping(row["Geschlecht(DE)"].ToString()) } into grp
                                  select new
                                  {
                                      PROD_NAME = string.Join("#", grp.Select(r => r["StyleName"]).Distinct()),
                                      DESCRIPTION = string.Join("#", grp.Select(r => r["ProductText(DE)"]).Distinct()),
                                      PROD_NUMBER = grp.Key.PROD_NUMBER,
                                      GENDER = grp.Key.GENDER,
                                      BASE_PRICE = grp.Key.BASE_PRICE,
                                      DigizuitePackshot = string.Join(";", grp.Select(r => r["DigizuitePackshot"]).Distinct()),
                                      DigizuitePackshot1 = string.Join(";", grp.Select(r => r["DigizuitePackshot1"]).Distinct()),
                                      DigizuitePackshot2 = string.Join(";", grp.Select(r => r["DigizuitePackshot2"]).Distinct()),
                                      DigizuitePackshot3 = string.Join(";", grp.Select(r => r["DigizuitePackshot3"]).Distinct()),
                                      DigizuitePackshot4 = string.Join(";", grp.Select(r => r["DigizuitePackshot4"]).Distinct()),
                                      DigizuitePackshot5 = string.Join(";", grp.Select(r => r["DigizuitePackshot5"]).Distinct()),
                                      DigizuitePackshot6 = string.Join(";", grp.Select(r => r["DigizuitePackshot6"]).Distinct()),
                                      EAN = string.Join(";", grp.Select(r => r["EAN"]).Distinct()),
                                      SIZE = string.Join(";", grp.Select(r => r["Größe"]).Distinct()),
                                      COLORCODE = string.Join(";", grp.Select(r => r["ColorCode"]).Distinct()),
                                      COLORNAME = string.Join(";", grp.Select(r => r["lookupColorName"]).Distinct())
                                  };


                string TableName = "EAN_DB";

                // Delete any existing data regarding selected brand
                int i = _dal.DeleteBrandFiles(TableName, vBrandID);


                DataTable EAN_DB_DATA = new DataTable();
                EAN_DB_DATA.Columns.Add("BRAND_ID");
                EAN_DB_DATA.Columns.Add("BRAND_NAME");
                EAN_DB_DATA.Columns.Add("PRODUCT_NAME");
                EAN_DB_DATA.Columns.Add("PRODUCT_NUMBER");
                EAN_DB_DATA.Columns.Add("PRODUCT_GENDER");
                EAN_DB_DATA.Columns.Add("PRICE_UVP");
                EAN_DB_DATA.Columns.Add("EAN");
                EAN_DB_DATA.Columns.Add("SIZE");
                EAN_DB_DATA.Columns.Add("COLOR_CODE");
                EAN_DB_DATA.Columns.Add("COLOR_NAME");
                EAN_DB_DATA.Columns.Add("IMAGE_URL"); EAN_DB_DATA.Columns.Add("STATUS");
                string ProdName = string.Empty;
                string Prodnumber = string.Empty;

                foreach (DataRow row in dataTable.Rows)
                {
                    if (row["LISTPRICEDEEUR"].ToString().Trim() == "" || row["LISTPRICEDEEUR"].ToString().Trim() == "0") continue;
                    if (Prodnumber != row["StyleNo"].ToString())
                    {
                        ProdName = row["StyleName"].ToString();
                        Prodnumber = row["StyleNo"].ToString();
                    }

                    var newRow = EAN_DB_DATA.NewRow();
                    newRow["BRAND_ID"] = vBrandID;
                    newRow["BRAND_NAME"] = vBrandName;
                    newRow["PRODUCT_NAME"] = replaceGermanUmlauts(ProdName);
                    newRow["PRODUCT_NUMBER"] = row["StyleNo"];
                    newRow["PRODUCT_GENDER"] = GenderMapping(row["Geschlecht(DE)"].ToString());
                    newRow["PRICE_UVP"] = row["LISTPRICEDEEUR"].ToString().Trim()?.Replace('.', ',');
                    newRow["EAN"] = row["EAN"];
                    newRow["SIZE"] = row["Größe"];
                    newRow["COLOR_CODE"] = row["ColorCode"];
                    newRow["COLOR_NAME"] = replaceGermanUmlauts(row["lookupColorName"].ToString());
                    Uri uriResult;
                    string uriName = row["DigizuitePackshot"].ToString();
                    //bool result = Uri.TryCreate(uriName, UriKind.Absolute, out uriResult) && (uriResult.Scheme == Uri.UriSchemeHttp || uriResult.Scheme == Uri.UriSchemeHttps);
                    newRow["IMAGE_URL"] = uriName;
                    EAN_DB_DATA.Rows.Add(newRow);
                }



                var config = _basicUtilities.GetConfiguration();
                string conString = config.GetSection("ConnectionStrings:sqlconnection").Value;
                using (SqlConnection con = new SqlConnection(conString))
                {

                    using (SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(con))
                    {

                        sqlBulkCopy.BulkCopyTimeout = 600;
                        sqlBulkCopy.DestinationTableName = "dbo." + TableName;
                        DataColumnCollection dataColumnCollection = EAN_DB_DATA.Columns;
                        for (int j = 0; j < dataColumnCollection.Count; j++)
                        {
                            string columnName = dataColumnCollection[j].ToString()?.Replace(" ", "");
                            sqlBulkCopy.ColumnMappings.Add(columnName, columnName);
                        }
                        con.Open();
                        sqlBulkCopy.WriteToServer(EAN_DB_DATA);
                        con.Close();
                    }
                }






                foreach (var item in groupedData)
                {
                    if (item.BASE_PRICE.ToString().Trim() == "" || item.BASE_PRICE.ToString().Trim() == "0") continue;
                    DataRow newRow = resultTable.NewRow();

                    newRow["BRANDID"] = vBrandID;
                    newRow["EAN"] = item.EAN.ToString().Trim();
                    newRow["BRAND"] = vBrandName;
                    newRow["LINE"] = "".ToString().Trim();
                    newRow["PROD_NAME"] = item.PROD_NAME.Split("#")[0].ToString().Trim();
                    newRow["PROD_NUMBER"] = item.PROD_NUMBER.ToString().Trim();
                    newRow["UNIFYING_PROD_ID"] = item.PROD_NUMBER.ToString().Trim();
                    newRow["TITLE"] = item.PROD_NAME.Split("#")[0].ToString().Trim();
                    string gender = GenderMapping(item.GENDER.ToString());
                    newRow["SEPERATING_PROD_ID"] = gender.Trim().Length > 0 ? item.PROD_NUMBER.ToString().Trim() + " - " + gender.Trim() : item.PROD_NUMBER.ToString().Trim();
                    newRow["PRODUCT_TYPE"] = "".ToString().Trim();
                    newRow["PROD_GENDER"] = gender.Trim();
                    newRow["PROD_DESCRIPTION"] = replaceGermanUmlauts(item.DESCRIPTION.Split("#")[0].ToString().Trim());
                    newRow["HTML_BODY"] = "".ToString().Trim();
                    newRow["VENDOR"] = "".ToString().Trim();
                    newRow["TAGS"] = "".ToString().Trim();
                    newRow["PUBLISHED"] = "".ToString().Trim();
                    newRow["MANUFACTURER_SIZE_SPECTRUM"] = item.SIZE.ToString().Trim();
                    newRow["STORE_SIZE_SPECTRUM"] = item.SIZE.ToString().Trim();
                    newRow["MANUFAC_COLOR_SPECTRUM_NAMES"] = item.COLORNAME.ToString().Trim();
                    newRow["MANUFAC_COLOR_SPECTRUM_CODES"] = item.COLORCODE.ToString().Trim();
                    newRow["STORE_COLOR_SPECTRUM"] = item.COLORNAME.ToString().Trim();
                    newRow["COLOR_SELECTION"] = "".ToString().Trim();
                    newRow["EXTRA_OPT_NAME"] = "".ToString().Trim();
                    newRow["EXTRA_OPT_VAL"] = "".ToString().Trim();
                    newRow["VERSION_NAME"] = "".ToString().Trim();
                    newRow["BASE_PRICE"] = item.BASE_PRICE.ToString().Trim()?.Replace('.', ',');
                    newRow["VARIANT_GRAMS"] = "".ToString().Trim();
                    newRow["VARIANT_INV_TRACKER"] = "".ToString().Trim();
                    newRow["VARIANT_INV_QTY"] = "".ToString().Trim();
                    newRow["VARIANT_INV_POLICY"] = "".ToString().Trim();
                    newRow["VARIANT_FULFILLMENT_SERVICE"] = "".ToString().Trim();
                    newRow["VARIANT_COMP_AT_PRICE"] = "".ToString().Trim();
                    newRow["VARIANT_REQ_SHIPPING"] = "".ToString().Trim();
                    newRow["VAR_TAXABLE"] = "".ToString().Trim();
                    newRow["VARIANT_BCODE"] = "".ToString().Trim();
                    newRow["IMAGE_POSITION"] = "".ToString().Trim();
                    newRow["IMAGE_ALT_TXT"] = "".ToString().Trim();
                    newRow["GIFT_CARD"] = "".ToString().Trim();
                    newRow["SEO_TITLE"] = "".ToString().Trim();

                    var items = new List<string>
                                {
                                    item.DigizuitePackshot?.ToString().Trim().TrimEnd(','),
                                    item.DigizuitePackshot1?.ToString().Trim().TrimEnd(','),
                                    item.DigizuitePackshot2?.ToString().Trim().TrimEnd(','),
                                    item.DigizuitePackshot3?.ToString().Trim().TrimEnd(','),
                                    item.DigizuitePackshot4?.ToString().Trim().TrimEnd(','),
                                    item.DigizuitePackshot5?.ToString().Trim().TrimEnd(','),
                                    item.DigizuitePackshot6?.ToString().Trim().TrimEnd(',')
                                };

                    newRow["VARIANT_IMAGE"] = "";
                    newRow["VARIANT_WEIGHT_UNIT"] = "".ToString().Trim();
                    newRow["VARIANT_TAX_CODE"] = "".ToString().Trim();
                    newRow["COST_PER_ITEM"] = "".ToString().Trim();
                    newRow["PRICE_INTERNATIONAL"] = "".ToString().Trim();
                    newRow["COMP_AT_PRICE_INTL"] = "".ToString().Trim();
                    newRow["STATUS"] = "".ToString().Trim();

                    string inputString = item.PROD_NAME.ToString().Trim().ToLower();
                    string stringWithHyphens = Regex.Replace(inputString, @"\s", "-");
                    string result = Regex.Replace(stringWithHyphens, @"[^\w\d;]", "");
                    result = replaceGermanUmlauts(result);
                    newRow["PROD_FILE_NAME"] = result;

                    inputString = item.COLORNAME.ToString().Trim().ToLower();
                    stringWithHyphens = Regex.Replace(inputString, @"\s", "-");
                    result = Regex.Replace(stringWithHyphens, @"[^\w\d;]", "");

                    newRow["COLOR_NAMES"] = ";" + replaceGermanUmlauts(result);
                    resultTable.Rows.Add(newRow);
                }

                dataTable = resultTable;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                dataTable = new DataTable();
            }
            return dataTable;

        }
        private DataTable ProcessErimaBrandFile(string vBrandID, string vBrandName, DataTable dt)
        {
            DataTable resultTable = new DataTable();
            DataTable dataTable = new DataTable();
            try
            {
                resultTable.Columns.Add("BRANDID");
                resultTable.Columns.Add("BRAND");
                resultTable.Columns.Add("LINE");
                resultTable.Columns.Add("PROD_NAME");
                resultTable.Columns.Add("PROD_NUMBER");
                resultTable.Columns.Add("UNIFYING_PROD_ID");
                resultTable.Columns.Add("SEPERATING_PROD_ID");
                resultTable.Columns.Add("TITLE");
                resultTable.Columns.Add("PRODUCT_TYPE");
                resultTable.Columns.Add("PROD_GENDER");
                resultTable.Columns.Add("EAN");
                resultTable.Columns.Add("PROD_DESCRIPTION");
                resultTable.Columns.Add("HTML_BODY");
                resultTable.Columns.Add("VENDOR");
                resultTable.Columns.Add("TAGS");
                resultTable.Columns.Add("PUBLISHED");
                resultTable.Columns.Add("MANUFACTURER_SIZE_SPECTRUM");
                resultTable.Columns.Add("STORE_SIZE_SPECTRUM");
                resultTable.Columns.Add("MANUFAC_COLOR_SPECTRUM_NAMES");
                resultTable.Columns.Add("MANUFAC_COLOR_SPECTRUM_CODES");
                resultTable.Columns.Add("STORE_COLOR_SPECTRUM");
                resultTable.Columns.Add("COLOR_SELECTION");
                resultTable.Columns.Add("EXTRA_OPT_NAME");
                resultTable.Columns.Add("EXTRA_OPT_VAL");
                resultTable.Columns.Add("VERSION_NAME");
                resultTable.Columns.Add("BASE_PRICE");
                resultTable.Columns.Add("VARIANT_GRAMS");
                resultTable.Columns.Add("VARIANT_INV_TRACKER");
                resultTable.Columns.Add("VARIANT_INV_QTY");
                resultTable.Columns.Add("VARIANT_INV_POLICY");
                resultTable.Columns.Add("VARIANT_FULFILLMENT_SERVICE");
                resultTable.Columns.Add("VARIANT_COMP_AT_PRICE");
                resultTable.Columns.Add("VARIANT_REQ_SHIPPING");
                resultTable.Columns.Add("VAR_TAXABLE");
                resultTable.Columns.Add("VARIANT_BCODE");
                resultTable.Columns.Add("IMAGE_POSITION");
                resultTable.Columns.Add("IMAGE_ALT_TXT");
                resultTable.Columns.Add("GIFT_CARD");
                resultTable.Columns.Add("SEO_TITLE");
                resultTable.Columns.Add("VARIANT_IMAGE");
                resultTable.Columns.Add("VARIANT_WEIGHT_UNIT");
                resultTable.Columns.Add("VARIANT_TAX_CODE");
                resultTable.Columns.Add("COST_PER_ITEM");
                resultTable.Columns.Add("PRICE_INTERNATIONAL");
                resultTable.Columns.Add("COMP_AT_PRICE_INTL");
                resultTable.Columns.Add("STATUS");
                resultTable.Columns.Add("PROD_FILE_NAME");
                resultTable.Columns.Add("COLOR_NAMES");

                //dataTable = dt.AsEnumerable()
                //  .OrderBy(row => row.Field<string>("Artikelnummer"))
                //  .ThenBy(row => GetSortValue(row.Field<object>("Groesse")))
                //  .CopyToDataTable();


                dataTable = SortDataTable(dt, "Artikelnummer", "Groesse", "FarbeDE");

                var groupedData = from row in dataTable.AsEnumerable()
                                  group row by new { PROD_NUMBER = row["Artikelnummer"], LINE = row["Linie"], TYPE = row["ProduktartDE"], BASE_PRICE = row["DEEmpfVKEUR"], GENDER = GenderMapping(row["ZielgruppeDE"].ToString()), DESCRIPTION = row["MarketingtextundUSPsDE"] } into grp
                                  select new
                                  {
                                      PROD_NAME = string.Join("#", grp.Select(r => r["ArtikelnameDE"]).Distinct()),
                                      DESCRIPTION = string.Join("#", grp.Select(r => r["MarketingtextundUSPsDE"]).Distinct()),
                                      PROD_NUMBER = grp.Key.PROD_NUMBER,
                                      TYPE = grp.Key.TYPE,
                                      LINE = grp.Key.LINE,
                                      GENDER = grp.Key.GENDER,
                                      BASE_PRICE = grp.Key.BASE_PRICE,
                                      EAN = string.Join(";", grp.Select(r => r["EANCode"]).Distinct()),
                                      SIZE = string.Join(";", grp.Select(r => r["Groesse"]).Distinct()),
                                      COLORCODE = string.Join(";", grp.Select(r => r["FarbeDE"]).Distinct()),
                                      COLORNAME = string.Join(";", grp.Select(r => r["FarbeDE"]).Distinct())
                                  };



                string TableName = "EAN_DB";

                // Delete any existing data regarding selected brand
                int i = _dal.DeleteBrandFiles(TableName, vBrandID);


                DataTable EAN_DB_DATA = new DataTable();
                EAN_DB_DATA.Columns.Add("BRAND_ID");
                EAN_DB_DATA.Columns.Add("BRAND_NAME");
                EAN_DB_DATA.Columns.Add("PRODUCT_NAME");
                EAN_DB_DATA.Columns.Add("PRODUCT_NUMBER");
                EAN_DB_DATA.Columns.Add("PRODUCT_GENDER");
                EAN_DB_DATA.Columns.Add("PRICE_UVP");
                EAN_DB_DATA.Columns.Add("EAN");
                EAN_DB_DATA.Columns.Add("SIZE");
                EAN_DB_DATA.Columns.Add("COLOR_CODE");
                EAN_DB_DATA.Columns.Add("COLOR_NAME");
                EAN_DB_DATA.Columns.Add("IMAGE_URL"); EAN_DB_DATA.Columns.Add("STATUS");

                string ProdName = string.Empty;
                string Prodnumber = string.Empty;

                foreach (DataRow row in dataTable.Rows)
                {
                    if (row["DEEmpfVKEUR"].ToString().Trim() == "" || row["DEEmpfVKEUR"].ToString().Trim() == "0") continue;

                    if (Prodnumber != row["Artikelnummer"].ToString())
                    {
                        ProdName = row["ArtikelnameDE"].ToString();
                        Prodnumber = row["Artikelnummer"].ToString();
                    }



                    var newRow = EAN_DB_DATA.NewRow();
                    newRow["BRAND_ID"] = vBrandID;
                    newRow["BRAND_NAME"] = vBrandName;
                    newRow["PRODUCT_NAME"] = replaceGermanUmlauts(ProdName);
                    newRow["PRODUCT_NUMBER"] = "E" + row["Artikelnummer"];
                    newRow["PRODUCT_GENDER"] = GenderMapping(row["ZielgruppeDE"].ToString());
                    newRow["PRICE_UVP"] = row["DEEmpfVKEUR"].ToString().Trim()?.Replace('.', ',');
                    newRow["EAN"] = row["EANCode"];
                    newRow["SIZE"] = row["Groesse"];
                    newRow["COLOR_CODE"] = 0;
                    newRow["COLOR_NAME"] = replaceGermanUmlauts(row["FarbeDE"].ToString());
                    newRow["IMAGE_URL"] = "";
                    EAN_DB_DATA.Rows.Add(newRow);
                }



                var config = _basicUtilities.GetConfiguration();
                string conString = config.GetSection("ConnectionStrings:sqlconnection").Value;
                using (SqlConnection con = new SqlConnection(conString))
                {

                    using (SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(con))
                    {

                        sqlBulkCopy.BulkCopyTimeout = 600;
                        sqlBulkCopy.DestinationTableName = "dbo." + TableName;
                        DataColumnCollection dataColumnCollection = EAN_DB_DATA.Columns;
                        for (int j = 0; j < dataColumnCollection.Count; j++)
                        {
                            string columnName = dataColumnCollection[j].ToString()?.Replace(" ", "");
                            sqlBulkCopy.ColumnMappings.Add(columnName, columnName);
                        }
                        con.Open();
                        sqlBulkCopy.WriteToServer(EAN_DB_DATA);
                        con.Close();
                    }
                }














                foreach (var item in groupedData)
                {
                    if (item.BASE_PRICE.ToString().Trim() == "0" || item.BASE_PRICE.ToString().Trim() == "") continue;

                    DataRow newRow = resultTable.NewRow();

                    newRow["BRANDID"] = vBrandID;
                    newRow["EAN"] = item.EAN.ToString().Trim();
                    newRow["BRAND"] = vBrandName;
                    newRow["LINE"] = item.LINE.ToString().Trim();
                    newRow["PROD_NAME"] = item.PROD_NAME.Split("#")[0].ToString().Trim();
                    newRow["PROD_NUMBER"] = "E" + item.PROD_NUMBER.ToString().Trim();
                    newRow["UNIFYING_PROD_ID"] = "E" + item.PROD_NUMBER.ToString().Trim();

                    string gender = GenderMapping(item.GENDER.ToString());
                    newRow["SEPERATING_PROD_ID"] = gender.Trim().Length > 0 ? "E" + item.PROD_NUMBER.ToString().Trim() + " - " + gender.Trim() : "E" + item.PROD_NUMBER.ToString().Trim();
                    newRow["TITLE"] = item.PROD_NAME.Split("#")[0].ToString().Trim();
                    newRow["PRODUCT_TYPE"] = item.TYPE.ToString().Trim();
                    newRow["PROD_GENDER"] = gender.Trim();
                    newRow["PROD_DESCRIPTION"] = replaceGermanUmlauts(item.DESCRIPTION.Split("#")[0].ToString().Trim());
                    newRow["HTML_BODY"] = "".ToString().Trim();
                    newRow["VENDOR"] = "".ToString().Trim();
                    newRow["TAGS"] = "".ToString().Trim();
                    newRow["PUBLISHED"] = "".ToString().Trim();
                    newRow["MANUFACTURER_SIZE_SPECTRUM"] = item.SIZE.ToString().Trim();
                    newRow["STORE_SIZE_SPECTRUM"] = item.SIZE.ToString().Trim();
                    newRow["MANUFAC_COLOR_SPECTRUM_NAMES"] = item.COLORNAME.ToString().Trim();
                    newRow["MANUFAC_COLOR_SPECTRUM_CODES"] = 0.ToString().Trim();
                    newRow["STORE_COLOR_SPECTRUM"] = 0.ToString().Trim();
                    newRow["COLOR_SELECTION"] = "".ToString().Trim();
                    newRow["EXTRA_OPT_NAME"] = "".ToString().Trim();
                    newRow["EXTRA_OPT_VAL"] = "".ToString().Trim();
                    newRow["VERSION_NAME"] = "".ToString().Trim();
                    newRow["BASE_PRICE"] = item.BASE_PRICE.ToString().Trim()?.Replace('.', ',');
                    newRow["VARIANT_GRAMS"] = "".ToString().Trim();
                    newRow["VARIANT_INV_TRACKER"] = "".ToString().Trim();
                    newRow["VARIANT_INV_QTY"] = "".ToString().Trim();
                    newRow["VARIANT_INV_POLICY"] = "".ToString().Trim();
                    newRow["VARIANT_FULFILLMENT_SERVICE"] = "".ToString().Trim();
                    newRow["VARIANT_COMP_AT_PRICE"] = "".ToString().Trim();
                    newRow["VARIANT_REQ_SHIPPING"] = "".ToString().Trim();
                    newRow["VAR_TAXABLE"] = "".ToString().Trim();
                    newRow["VARIANT_BCODE"] = "".ToString().Trim();
                    newRow["IMAGE_POSITION"] = "".ToString().Trim();
                    newRow["IMAGE_ALT_TXT"] = "".ToString().Trim();
                    newRow["GIFT_CARD"] = "".ToString().Trim();
                    newRow["SEO_TITLE"] = "".ToString().Trim();
                    newRow["VARIANT_IMAGE"] = "".ToString().Trim();
                    newRow["VARIANT_WEIGHT_UNIT"] = "".ToString().Trim();
                    newRow["VARIANT_TAX_CODE"] = "".ToString().Trim();
                    newRow["COST_PER_ITEM"] = "".ToString().Trim();
                    newRow["PRICE_INTERNATIONAL"] = "".ToString().Trim();
                    newRow["COMP_AT_PRICE_INTL"] = "".ToString().Trim();
                    newRow["STATUS"] = "".ToString().Trim();

                    string inputString = item.PROD_NAME.ToString().Trim().ToLower();
                    string stringWithHyphens = Regex.Replace(inputString, @"\s", "-");
                    string result = Regex.Replace(stringWithHyphens, @"[^\w\d;]", "");
                    result = replaceGermanUmlauts(result);
                    newRow["PROD_FILE_NAME"] = result;

                    inputString = item.COLORNAME.ToString().Trim().ToLower();
                    stringWithHyphens = Regex.Replace(inputString, @"\s", "-");
                    result = Regex.Replace(stringWithHyphens, @"[^\w\d;]", "");

                    newRow["COLOR_NAMES"] = ";" + replaceGermanUmlauts(result);
                    resultTable.Rows.Add(newRow);
                }

                dataTable = resultTable;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                dataTable = new DataTable();
            }
            return dataTable;

        }


        private DataTable SortDataTable(DataTable dt, string article, string size, string color)
        {
            /*DataTable dataTable = dt.Clone();
            var integerRows = new List<DataRow>();
            var stringRows = new List<DataRow>();
            var stringRows1 = new List<DataRow>();
            var stringRows2 = new List<DataRow>();
            var stringRows3 = new List<DataRow>();
            var stringRows4 = new List<DataRow>();

            foreach (DataRow row in dt.Rows)
            {
                var sizeValue = row.Field<string>(size);
                float intSize;
                NumberStyles style;
                CultureInfo culture;
                style = NumberStyles.AllowDecimalPoint;
                culture = CultureInfo.CreateSpecificCulture("de-DE");


                if (float.TryParse(sizeValue, style, culture, out intSize))
                {
                    integerRows.Add(row);
                }
                else
                {
                    stringRows.Add(row);
                }
            }


            var sortedIntegerRows = integerRows.OrderBy(row => row[article])
                .ThenBy(row => Convert.ToDecimal(row[size], CultureInfo.GetCultureInfo("de-DE")))
                .ThenBy(row => row[color]);

            var sortedStringRows = stringRows.OrderBy(row => row[article])
                .ThenBy(row => row[size].ToString())
                .ThenBy(row => row[color]);          

            foreach (var row in sortedIntegerRows.Concat(sortedStringRows))
            {
                dataTable.ImportRow(row);
            }
            return dataTable;*/



            try
            {
                DataTable mainDataTable = dt;
                DataTable sizeReferenceTable = _dal.GET_SORTED_SIZE();
                var sizeIndex = new Dictionary<string, int>();
                for (int i = 0; i < sizeReferenceTable.Rows.Count; i++)
                {
                    string sized = sizeReferenceTable.Rows[i].Field<string>("Size");
                    // Check for null or empty strings in the "Size" column
                    if (!string.IsNullOrEmpty(sized))
                    {
                        sizeIndex[sized] = i;
                    }
                    // Handle null or empty values (customize as per your requirement)
                    else
                    {
                        // Assign a default index or handle null values in a way that fits your sorting logic
                        sizeIndex["Default"] = int.MaxValue;
                    }
                }
                mainDataTable = mainDataTable.AsEnumerable()
                                .OrderBy(row => row.Field<string>(article))
                                .ThenBy(row =>
                                {
                                    string sized = row.Field<string>(size) == "" || row.Field<string>(size) == null || row.Field<string>(size) == "null" ? "" : row.Field<string>(size);
                                    if (sized == "")
                                    {
                                        return int.MaxValue;
                                    }
                                    else
                                    {

                                        return sizeIndex.ContainsKey(sized) ? sizeIndex[sized] : int.MaxValue;
                                    }
                                })
                                .ThenBy(row => row.Field<string>(color))
                                .CopyToDataTable();

                return mainDataTable;

            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                return new DataTable();
            }


        }
        private DataTable ProcessJakoBrandFile(string vBrandID, string vBrandName, DataTable dt)
        {
            DataTable resultTable = new DataTable();
            DataTable dataTable = new DataTable();
            try
            {
                resultTable.Columns.Add("BRANDID");
                resultTable.Columns.Add("BRAND");
                resultTable.Columns.Add("LINE");
                resultTable.Columns.Add("PROD_NAME");
                resultTable.Columns.Add("PROD_NUMBER");
                resultTable.Columns.Add("UNIFYING_PROD_ID");
                resultTable.Columns.Add("SEPERATING_PROD_ID");
                resultTable.Columns.Add("TITLE");
                resultTable.Columns.Add("PRODUCT_TYPE");
                resultTable.Columns.Add("PROD_GENDER");
                resultTable.Columns.Add("EAN");
                resultTable.Columns.Add("PROD_DESCRIPTION");
                resultTable.Columns.Add("HTML_BODY");
                resultTable.Columns.Add("VENDOR");
                resultTable.Columns.Add("TAGS");
                resultTable.Columns.Add("PUBLISHED");
                resultTable.Columns.Add("MANUFACTURER_SIZE_SPECTRUM");
                resultTable.Columns.Add("STORE_SIZE_SPECTRUM");
                resultTable.Columns.Add("MANUFAC_COLOR_SPECTRUM_NAMES");
                resultTable.Columns.Add("MANUFAC_COLOR_SPECTRUM_CODES");
                resultTable.Columns.Add("STORE_COLOR_SPECTRUM");
                resultTable.Columns.Add("COLOR_SELECTION");
                resultTable.Columns.Add("EXTRA_OPT_NAME");
                resultTable.Columns.Add("EXTRA_OPT_VAL");
                resultTable.Columns.Add("VERSION_NAME");
                resultTable.Columns.Add("BASE_PRICE");
                resultTable.Columns.Add("VARIANT_GRAMS");
                resultTable.Columns.Add("VARIANT_INV_TRACKER");
                resultTable.Columns.Add("VARIANT_INV_QTY");
                resultTable.Columns.Add("VARIANT_INV_POLICY");
                resultTable.Columns.Add("VARIANT_FULFILLMENT_SERVICE");
                resultTable.Columns.Add("VARIANT_COMP_AT_PRICE");
                resultTable.Columns.Add("VARIANT_REQ_SHIPPING");
                resultTable.Columns.Add("VAR_TAXABLE");
                resultTable.Columns.Add("VARIANT_BCODE");
                resultTable.Columns.Add("IMAGE_POSITION");
                resultTable.Columns.Add("IMAGE_ALT_TXT");
                resultTable.Columns.Add("GIFT_CARD");
                resultTable.Columns.Add("SEO_TITLE");
                resultTable.Columns.Add("VARIANT_IMAGE");
                resultTable.Columns.Add("VARIANT_WEIGHT_UNIT");
                resultTable.Columns.Add("VARIANT_TAX_CODE");
                resultTable.Columns.Add("COST_PER_ITEM");
                resultTable.Columns.Add("PRICE_INTERNATIONAL");
                resultTable.Columns.Add("COMP_AT_PRICE_INTL");
                resultTable.Columns.Add("STATUS");
                resultTable.Columns.Add("PROD_FILE_NAME");
                resultTable.Columns.Add("COLOR_NAMES");



                dataTable = SortDataTable(dt, "ItemNo", "SIZE", "ColorDescription");




                string TableName = "EAN_DB";

                // Delete any existing data regarding selected brand
                int i = _dal.DeleteBrandFiles(TableName, vBrandID);


                DataTable EAN_DB_DATA = new DataTable();
                EAN_DB_DATA.Columns.Add("BRAND_ID");
                EAN_DB_DATA.Columns.Add("BRAND_NAME");
                EAN_DB_DATA.Columns.Add("PRODUCT_NAME");
                EAN_DB_DATA.Columns.Add("PRODUCT_NUMBER");
                EAN_DB_DATA.Columns.Add("PRODUCT_GENDER");
                EAN_DB_DATA.Columns.Add("PRICE_UVP");
                EAN_DB_DATA.Columns.Add("EAN");
                EAN_DB_DATA.Columns.Add("SIZE");
                EAN_DB_DATA.Columns.Add("COLOR_CODE");
                EAN_DB_DATA.Columns.Add("COLOR_NAME");
                EAN_DB_DATA.Columns.Add("IMAGE_URL"); EAN_DB_DATA.Columns.Add("STATUS");

                string ProdName = string.Empty;
                string Prodnumber = string.Empty;

                foreach (DataRow row in dataTable.Rows)
                {
                    if (row["UVP"].ToString().Trim() == "" || row["UVP"].ToString().Trim() == "0") continue;
                    if (Prodnumber != row["ItemNo"].ToString())
                    {
                        ProdName = row["Description"].ToString();
                        Prodnumber = row["ItemNo"].ToString();
                    }


                    var newRow = EAN_DB_DATA.NewRow();
                    newRow["BRAND_ID"] = vBrandID;
                    newRow["BRAND_NAME"] = vBrandName;
                    newRow["PRODUCT_NAME"] = replaceGermanUmlauts(ProdName);
                    newRow["PRODUCT_NUMBER"] = row["ItemNo"];
                    newRow["PRODUCT_GENDER"] = GenderMapping(row["GENDER"].ToString());
                    newRow["PRICE_UVP"] = row["UVP"].ToString().Trim()?.Replace('.', ',');
                    newRow["EAN"] = row["EAN"];
                    newRow["SIZE"] = row["SIZE"];
                    newRow["COLOR_CODE"] = row["ColorCode"];
                    newRow["COLOR_NAME"] = replaceGermanUmlauts(row["ColorDescription"].ToString());
                    newRow["IMAGE_URL"] = "";
                    newRow["STATUS"] = 0;
                    EAN_DB_DATA.Rows.Add(newRow);
                }



                var config = _basicUtilities.GetConfiguration();
                string conString = config.GetSection("ConnectionStrings:sqlconnection").Value;
                using (SqlConnection con = new SqlConnection(conString))
                {

                    using (SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(con))
                    {

                        sqlBulkCopy.BulkCopyTimeout = 600;
                        sqlBulkCopy.DestinationTableName = "dbo." + TableName;
                        DataColumnCollection dataColumnCollection = EAN_DB_DATA.Columns;
                        for (int j = 0; j < dataColumnCollection.Count; j++)
                        {
                            string columnName = dataColumnCollection[j].ToString()?.Replace(" ", "");
                            sqlBulkCopy.ColumnMappings.Add(columnName, columnName);
                        }
                        con.Open();
                        sqlBulkCopy.WriteToServer(EAN_DB_DATA);
                        con.Close();
                    }
                }























                var groupedData = from row in dataTable.AsEnumerable()
                                  group row by new { PROD_NUMBER = row["ItemNo"], BASE_PRICE = row["UVP"], GENDER = GenderMapping(row["GENDER"].ToString()), N = row["recommendedUVP"], M = row["PriceIndividual"], O = row["Text1"], P = row["Text2"], Q = row["Text3"], R = row["Text4"], S = row["Text5"] } into grp
                                  select new
                                  {
                                      PROD_NAME = string.Join("#", grp.Select(r => r["Description"]).Distinct()),
                                      DESCRIPTION = string.Join("#", grp.Select(r => r["recommendedUVP"].ToString() + r["PriceIndividual"].ToString() + r["Text1"].ToString() + r["Text2"].ToString() + r["Text3"].ToString() + r["Text4"].ToString() + r["Text5"].ToString()).Distinct()),
                                      PROD_NUMBER = grp.Key.PROD_NUMBER,
                                      GENDER = grp.Key.GENDER,
                                      BASE_PRICE = grp.Key.BASE_PRICE,
                                      EAN = string.Join(";", grp.Select(r => r["EAN"]).Distinct()),
                                      SIZE = string.Join(";", grp.Select(r => r["SIZE"]).Distinct()),
                                      COLORCODE = string.Join(";", grp.Select(r => r["ColorCode"]).Distinct()),
                                      COLORNAME = string.Join(";", grp.Select(r => r["ColorDescription"]).Distinct())
                                  };




                foreach (var item in groupedData)
                {
                    if (item.BASE_PRICE.ToString().Trim() == "0" || item.BASE_PRICE.ToString().Trim() == "") continue;

                    DataRow newRow = resultTable.NewRow();
                    newRow["BRANDID"] = vBrandID;
                    newRow["EAN"] = item.EAN.ToString().Trim();
                    newRow["BRAND"] = vBrandName;
                    newRow["LINE"] = "";
                    newRow["PROD_NAME"] = item.PROD_NAME.Split("#")[0].ToString().Trim();
                    newRow["PROD_NUMBER"] = item.PROD_NUMBER.ToString().Trim();
                    newRow["UNIFYING_PROD_ID"] = item.PROD_NUMBER.ToString().Trim();
                    string gender = GenderMapping(item.GENDER.ToString());
                    newRow["SEPERATING_PROD_ID"] = gender.Trim().Length > 0 ? item.PROD_NUMBER.ToString().Trim() + " - " + gender.Trim() : item.PROD_NUMBER.ToString().Trim();
                    newRow["TITLE"] = item.PROD_NAME.Split("#")[0].ToString().Trim();
                    newRow["PRODUCT_TYPE"] = "".ToString().Trim();
                    newRow["PROD_GENDER"] = gender.Trim();
                    newRow["PROD_DESCRIPTION"] = replaceGermanUmlauts(item.DESCRIPTION.Split("#")[0].ToString().Trim());
                    newRow["HTML_BODY"] = "".ToString().Trim();
                    newRow["VENDOR"] = "".ToString().Trim();
                    newRow["TAGS"] = "".ToString().Trim();
                    newRow["PUBLISHED"] = "".ToString().Trim();
                    newRow["MANUFACTURER_SIZE_SPECTRUM"] = item.SIZE.ToString().Trim();
                    newRow["STORE_SIZE_SPECTRUM"] = item.SIZE.ToString().Trim();
                    newRow["MANUFAC_COLOR_SPECTRUM_NAMES"] = item.COLORNAME.ToString().Trim();
                    newRow["MANUFAC_COLOR_SPECTRUM_CODES"] = item.COLORCODE.ToString().Trim();
                    newRow["STORE_COLOR_SPECTRUM"] = item.COLORNAME.ToString().Trim();
                    newRow["COLOR_SELECTION"] = "".ToString().Trim();
                    newRow["EXTRA_OPT_NAME"] = "".ToString().Trim();
                    newRow["EXTRA_OPT_VAL"] = "".ToString().Trim();
                    newRow["VERSION_NAME"] = "".ToString().Trim();
                    newRow["BASE_PRICE"] = item.BASE_PRICE.ToString().Trim()?.Replace('.', ',');
                    newRow["VARIANT_GRAMS"] = "".ToString().Trim();
                    newRow["VARIANT_INV_TRACKER"] = "".ToString().Trim();
                    newRow["VARIANT_INV_QTY"] = "".ToString().Trim();
                    newRow["VARIANT_INV_POLICY"] = "".ToString().Trim();
                    newRow["VARIANT_FULFILLMENT_SERVICE"] = "".ToString().Trim();
                    newRow["VARIANT_COMP_AT_PRICE"] = "".ToString().Trim();
                    newRow["VARIANT_REQ_SHIPPING"] = "".ToString().Trim();
                    newRow["VAR_TAXABLE"] = "".ToString().Trim();
                    newRow["VARIANT_BCODE"] = "".ToString().Trim();
                    newRow["IMAGE_POSITION"] = "".ToString().Trim();
                    newRow["IMAGE_ALT_TXT"] = "".ToString().Trim();
                    newRow["GIFT_CARD"] = "".ToString().Trim();
                    newRow["SEO_TITLE"] = "".ToString().Trim();
                    newRow["VARIANT_IMAGE"] = "".ToString().Trim();
                    newRow["VARIANT_WEIGHT_UNIT"] = "".ToString().Trim();
                    newRow["VARIANT_TAX_CODE"] = "".ToString().Trim();
                    newRow["COST_PER_ITEM"] = "".ToString().Trim();
                    newRow["PRICE_INTERNATIONAL"] = "".ToString().Trim();
                    newRow["COMP_AT_PRICE_INTL"] = "".ToString().Trim();
                    newRow["STATUS"] = "".ToString().Trim();

                    string inputString = item.PROD_NAME.ToString().Trim().ToLower();
                    string stringWithHyphens = Regex.Replace(inputString, @"\s", "-");
                    string result = Regex.Replace(stringWithHyphens, @"[^\w\d;]", "");
                    result = replaceGermanUmlauts(result);
                    newRow["PROD_FILE_NAME"] = result;

                    inputString = item.COLORNAME.ToString().Trim().ToLower();
                    stringWithHyphens = Regex.Replace(inputString, @"\s", "-");
                    result = Regex.Replace(stringWithHyphens, @"[^\w\d;]", "");

                    newRow["COLOR_NAMES"] = ";" + replaceGermanUmlauts(result);
                    resultTable.Rows.Add(newRow);
                }

                dataTable = resultTable;

            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                dataTable = new DataTable();
            }
            return dataTable;





        }
        private DataTable ProcessPumaBrandFile(string vBrandID, string vBrandName, DataTable dt)
        {
            DataTable resultTable = new DataTable();
            DataTable dataTable = new DataTable();
            try
            {
                resultTable.Columns.Add("BRANDID");
                resultTable.Columns.Add("BRAND");
                resultTable.Columns.Add("LINE");
                resultTable.Columns.Add("PROD_NAME");
                resultTable.Columns.Add("PROD_NUMBER");
                resultTable.Columns.Add("UNIFYING_PROD_ID");
                resultTable.Columns.Add("SEPERATING_PROD_ID");
                resultTable.Columns.Add("TITLE");
                resultTable.Columns.Add("PRODUCT_TYPE");
                resultTable.Columns.Add("PROD_GENDER");
                resultTable.Columns.Add("EAN");
                resultTable.Columns.Add("PROD_DESCRIPTION");
                resultTable.Columns.Add("HTML_BODY");
                resultTable.Columns.Add("VENDOR");
                resultTable.Columns.Add("TAGS");
                resultTable.Columns.Add("PUBLISHED");
                resultTable.Columns.Add("MANUFACTURER_SIZE_SPECTRUM");
                resultTable.Columns.Add("STORE_SIZE_SPECTRUM");
                resultTable.Columns.Add("MANUFAC_COLOR_SPECTRUM_NAMES");
                resultTable.Columns.Add("MANUFAC_COLOR_SPECTRUM_CODES");
                resultTable.Columns.Add("STORE_COLOR_SPECTRUM");
                resultTable.Columns.Add("COLOR_SELECTION");
                resultTable.Columns.Add("EXTRA_OPT_NAME");
                resultTable.Columns.Add("EXTRA_OPT_VAL");
                resultTable.Columns.Add("VERSION_NAME");
                resultTable.Columns.Add("BASE_PRICE");
                resultTable.Columns.Add("VARIANT_GRAMS");
                resultTable.Columns.Add("VARIANT_INV_TRACKER");
                resultTable.Columns.Add("VARIANT_INV_QTY");
                resultTable.Columns.Add("VARIANT_INV_POLICY");
                resultTable.Columns.Add("VARIANT_FULFILLMENT_SERVICE");
                resultTable.Columns.Add("VARIANT_COMP_AT_PRICE");
                resultTable.Columns.Add("VARIANT_REQ_SHIPPING");
                resultTable.Columns.Add("VAR_TAXABLE");
                resultTable.Columns.Add("VARIANT_BCODE");
                resultTable.Columns.Add("IMAGE_POSITION");
                resultTable.Columns.Add("IMAGE_ALT_TXT");
                resultTable.Columns.Add("GIFT_CARD");
                resultTable.Columns.Add("SEO_TITLE");
                resultTable.Columns.Add("VARIANT_IMAGE");
                resultTable.Columns.Add("VARIANT_WEIGHT_UNIT");
                resultTable.Columns.Add("VARIANT_TAX_CODE");
                resultTable.Columns.Add("COST_PER_ITEM");
                resultTable.Columns.Add("PRICE_INTERNATIONAL");
                resultTable.Columns.Add("COMP_AT_PRICE_INTL");
                resultTable.Columns.Add("STATUS");
                resultTable.Columns.Add("PROD_FILE_NAME");
                resultTable.Columns.Add("COLOR_NAMES");

                //dataTable = dt.AsEnumerable()
                //  .OrderBy(row => row.Field<string>("STYLE"))
                //  .ThenBy(row => row.Field<string>("SIZE"))
                //  .CopyToDataTable();


                dataTable = SortDataTable(dt, "STYLE", "SIZE", "COLOR_NAME");


                string TableName = "EAN_DB";

                // Delete any existing data regarding selected brand
                int i = _dal.DeleteBrandFiles(TableName, vBrandID);


                DataTable EAN_DB_DATA = new DataTable();
                EAN_DB_DATA.Columns.Add("BRAND_ID");
                EAN_DB_DATA.Columns.Add("BRAND_NAME");
                EAN_DB_DATA.Columns.Add("PRODUCT_NAME");
                EAN_DB_DATA.Columns.Add("PRODUCT_NUMBER");
                EAN_DB_DATA.Columns.Add("PRODUCT_GENDER");
                EAN_DB_DATA.Columns.Add("PRICE_UVP");
                EAN_DB_DATA.Columns.Add("EAN");
                EAN_DB_DATA.Columns.Add("SIZE");
                EAN_DB_DATA.Columns.Add("COLOR_CODE");
                EAN_DB_DATA.Columns.Add("COLOR_NAME");
                EAN_DB_DATA.Columns.Add("IMAGE_URL"); EAN_DB_DATA.Columns.Add("STATUS");

                string ProdName = string.Empty;
                string Prodnumber = string.Empty;

                foreach (DataRow row in dataTable.Rows)
                {



                    if (row["UVP_DE"].ToString().Trim() == "" || row["UVP_DE"].ToString().Trim() == "0") continue;
                    if (Prodnumber != row["STYLE"].ToString())
                    {
                        ProdName = row["STYLE_NAME"].ToString();
                        Prodnumber = row["STYLE"].ToString();
                    }

                    var newRow = EAN_DB_DATA.NewRow();
                    newRow["BRAND_ID"] = vBrandID;
                    newRow["BRAND_NAME"] = vBrandName;
                    newRow["PRODUCT_NAME"] = replaceGermanUmlauts(ProdName);
                    newRow["PRODUCT_NUMBER"] = row["STYLE"];
                    newRow["PRODUCT_GENDER"] = GenderMapping(row["GENDER"].ToString());
                    newRow["PRICE_UVP"] = row["UVP_DE"].ToString().Trim()?.Replace('.', ',');
                    newRow["EAN"] = row["EAN"];
                    newRow["SIZE"] = row["SIZE"];
                    newRow["COLOR_CODE"] = row["COLOR"];
                    newRow["COLOR_NAME"] = replaceGermanUmlauts(row["COLOR_NAME"].ToString());
                    newRow["IMAGE_URL"] = row["BILDLINK"];
                    EAN_DB_DATA.Rows.Add(newRow);
                }



                var config = _basicUtilities.GetConfiguration();
                string conString = config.GetSection("ConnectionStrings:sqlconnection").Value;
                using (SqlConnection con = new SqlConnection(conString))
                {

                    using (SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(con))
                    {

                        sqlBulkCopy.BulkCopyTimeout = 600;
                        sqlBulkCopy.DestinationTableName = "dbo." + TableName;
                        DataColumnCollection dataColumnCollection = EAN_DB_DATA.Columns;
                        for (int j = 0; j < dataColumnCollection.Count; j++)
                        {
                            string columnName = dataColumnCollection[j].ToString()?.Replace(" ", "");
                            sqlBulkCopy.ColumnMappings.Add(columnName, columnName);
                        }
                        con.Open();
                        sqlBulkCopy.WriteToServer(EAN_DB_DATA);
                        con.Close();
                    }
                }

                var groupedData = from row in dataTable.AsEnumerable()
                                  group row by new { PROD_NAME = row["STYLE_NAME"], PROD_NUMBER = row["STYLE"], TITLE = row["STYLE_NAME"], BASE_PRICE = row["UVP_DE"], GENDER = GenderMapping(row["GENDER"].ToString()), PRODUCT_DIVISION = row["PRODUCT_DIVISION"] } into grp
                                  select new
                                  {
                                      PROD_NAME = string.Join("#", grp.Select(r => r["STYLE_NAME"]).Distinct()),
                                      PROD_NUMBER = grp.Key.PROD_NUMBER,
                                      TITLE = grp.Key.TITLE,
                                      GENDER = grp.Key.GENDER,
                                      PRODUCT_DIVISION = grp.Key.PRODUCT_DIVISION,
                                      BASE_PRICE = grp.Key.BASE_PRICE,
                                      AGE_GROUP = string.Join(";", grp.Select(r => r["AGE_GROUP"]).Distinct()),
                                      EAN = string.Join(";", grp.Select(r => r["EAN"]).Distinct()),
                                      SIZE = string.Join(";", grp.Select(r => r["SIZE"]).Distinct()),
                                      COLORCODE = string.Join(";", grp.Select(r => r["COLOR"]).Distinct()),
                                      COLORNAME = string.Join(";", grp.Select(r => r["COLOR_NAME"]).Distinct()),
                                      BILDLINK = string.Join(";", grp.Select(r => r["BILDLINK"]).Distinct())
                                  };

                foreach (var item in groupedData)
                {
                    if (item.BASE_PRICE.ToString().Trim() == "0" || item.BASE_PRICE.ToString().Trim() == "") continue;

                    if (vBrandName == "PUMA" && item.PRODUCT_DIVISION.ToString().ToLower().Contains("footwear")) continue;

                    DataRow newRow = resultTable.NewRow();

                    newRow["BRANDID"] = vBrandID;
                    newRow["EAN"] = item.EAN.ToString().Trim();
                    newRow["BRAND"] = vBrandName;
                    newRow["LINE"] = "";
                    newRow["PROD_NAME"] = item.PROD_NAME.Split("#")[0].ToString().Trim();
                    newRow["PROD_NUMBER"] = item.PROD_NUMBER.ToString().Trim();
                    newRow["UNIFYING_PROD_ID"] = item.PROD_NUMBER.ToString().Trim();
                    string gender = GenderMapping(item.GENDER.ToString().Trim());
                    newRow["SEPERATING_PROD_ID"] = gender.ToString().Trim().Length > 0 ? item.PROD_NUMBER.ToString().Trim() + " - " + gender : item.PROD_NUMBER.ToString().Trim();
                    newRow["TITLE"] = item.PROD_NAME.Split("#")[0].ToString().Trim();
                    newRow["PRODUCT_TYPE"] = "".ToString().Trim();
                    newRow["PROD_GENDER"] = gender.Trim();
                    newRow["PROD_DESCRIPTION"] = "".ToString().Trim();
                    newRow["HTML_BODY"] = "".ToString().Trim();
                    newRow["VENDOR"] = "".ToString().Trim();
                    newRow["TAGS"] = "".ToString().Trim();
                    newRow["PUBLISHED"] = "".ToString().Trim();
                    newRow["MANUFACTURER_SIZE_SPECTRUM"] = item.SIZE.ToString().Trim();
                    newRow["STORE_SIZE_SPECTRUM"] = item.SIZE.ToString().Trim();
                    newRow["MANUFAC_COLOR_SPECTRUM_NAMES"] = item.COLORNAME.ToString().Trim();
                    newRow["MANUFAC_COLOR_SPECTRUM_CODES"] = item.COLORCODE.ToString().Trim();
                    newRow["STORE_COLOR_SPECTRUM"] = item.COLORNAME.ToString().Trim();
                    newRow["COLOR_SELECTION"] = "".ToString().Trim();
                    newRow["EXTRA_OPT_NAME"] = "".ToString().Trim();
                    newRow["EXTRA_OPT_VAL"] = "".ToString().Trim();
                    newRow["VERSION_NAME"] = "".ToString().Trim();
                    newRow["BASE_PRICE"] = item.BASE_PRICE.ToString().Trim()?.Replace('.', ',');
                    newRow["VARIANT_GRAMS"] = "".ToString().Trim();
                    newRow["VARIANT_INV_TRACKER"] = "".ToString().Trim();
                    newRow["VARIANT_INV_QTY"] = "".ToString().Trim();
                    newRow["VARIANT_INV_POLICY"] = "".ToString().Trim();
                    newRow["VARIANT_FULFILLMENT_SERVICE"] = "".ToString().Trim();
                    newRow["VARIANT_COMP_AT_PRICE"] = "".ToString().Trim();
                    newRow["VARIANT_REQ_SHIPPING"] = "".ToString().Trim();
                    newRow["VAR_TAXABLE"] = "".ToString().Trim();
                    newRow["VARIANT_BCODE"] = "".ToString().Trim();
                    newRow["IMAGE_POSITION"] = "".ToString().Trim();
                    newRow["IMAGE_ALT_TXT"] = "".ToString().Trim();
                    newRow["GIFT_CARD"] = "".ToString().Trim();
                    newRow["SEO_TITLE"] = "".ToString().Trim();
                    newRow["VARIANT_IMAGE"] = item.BILDLINK.ToString().Trim().TrimEnd(',');
                    newRow["VARIANT_WEIGHT_UNIT"] = "".ToString().Trim();
                    newRow["VARIANT_TAX_CODE"] = "".ToString().Trim();
                    newRow["COST_PER_ITEM"] = "".ToString().Trim();
                    newRow["PRICE_INTERNATIONAL"] = "".ToString().Trim();
                    newRow["COMP_AT_PRICE_INTL"] = "".ToString().Trim();
                    newRow["STATUS"] = "".ToString().Trim();

                    string inputString = item.PROD_NAME.ToString().Trim().ToLower();
                    string stringWithHyphens = Regex.Replace(inputString, @"\s", "-");
                    string result = Regex.Replace(stringWithHyphens, @"[^\w\d;]", "");
                    result = replaceGermanUmlauts(result);
                    newRow["PROD_FILE_NAME"] = result;

                    inputString = item.COLORNAME.ToString().Trim().ToLower();
                    stringWithHyphens = Regex.Replace(inputString, @"\s", "-");
                    result = Regex.Replace(stringWithHyphens, @"[^\w\d;]", "");

                    newRow["COLOR_NAMES"] = ";" + replaceGermanUmlauts(result);
                    resultTable.Rows.Add(newRow);
                }

                dataTable = resultTable;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                dataTable = new DataTable();
            }
            return dataTable;

        }

        private string replaceGermanUmlauts(string s)
        {
            string t = s;
            t = t.Replace("ä", "ae");
            t = t.Replace("ö", "oe");
            t = t.Replace("ü", "ue");
            t = t.Replace("ß", "ss");
            t = t.Replace("Ä", "ae");
            t = t.Replace("Ö", "oe");
            t = t.Replace("Ü", "ue");
            t = t.Replace("ß", "ss");
            return t;
        }


        public async Task<bool> UploadDataToSheet(DataTable dt, string brandName, string _OP)
        {
            try
            {
                List<IList<object>> data = _basicUtilities.GetListObject(dt);
                string[] Scopes = { SheetsService.Scope.Spreadsheets };
                string ApplicationName = "My Team Shop";
                string jsonCredentialsPath = "credentials.json";
                GoogleCredential credential;
                using (var stream = new System.IO.FileStream(jsonCredentialsPath, System.IO.FileMode.Open, System.IO.FileAccess.Read))
                {
                    credential = GoogleCredential.FromStream(stream).CreateScoped(Scopes);
                }

                var service = new SheetsService(new BaseClientService.Initializer()
                {
                    HttpClientInitializer = credential,
                    ApplicationName = ApplicationName,
                });
                var config = _basicUtilities.GetConfiguration();

                string spreadsheetId = config.GetSection("SpreadSheetID").Value.ToString();
                string SheetName = config.GetSection("SheetName").Value.ToString();
                string range = SheetName + "!D4:AZ";

                int columnIndexToDelete = Convert.ToInt32(config.GetSection("columnIndexToDelete").Value.ToString());
                string valueToDelete = brandName.Trim();
                SpreadsheetsResource.ValuesResource.GetRequest request1 =
                service.Spreadsheets.Values.Get(spreadsheetId, range);
                ValueRange response1 = await request1.ExecuteAsync();
                IList<IList<Object>> values = response1.Values;
                List<IList<object>> mergedList = new List<IList<object>>();
                if (values != null && _OP == "Replace_Data")
                {
                    values = values.Where(row => !row[columnIndexToDelete].ToString().ToLower().Contains(valueToDelete.ToLower())).ToList();
                    mergedList = values.Concat(data).ToList();
                }
                else if (values != null && _OP == "Keep_All")
                {
                    mergedList = values.Concat(data).ToList();
                }
                else { mergedList = data; }

                ValueRange valueRange = new ValueRange();
                valueRange.Values = mergedList;
                var updateRequest = service.Spreadsheets.Values.Update(valueRange, spreadsheetId, range);
                updateRequest.ValueInputOption = SpreadsheetsResource.ValuesResource.UpdateRequest.ValueInputOptionEnum.RAW;
                var updateResponse = await updateRequest.ExecuteAsync();
                Console.WriteLine(updateResponse.ToString());
                return true;
            }
            catch (Exception r)
            {
                Console.WriteLine(r);
                return false;
            }
        }


        public async Task<bool> UploadDataToEANSheet(DataTable dt, string brandName, string _OP, bool check)
        {
            try
            {
                List<IList<object>> data = _basicUtilities.GetListObject(dt);
                string[] Scopes = { SheetsService.Scope.Spreadsheets };
                string ApplicationName = "My Team Shop";
                string jsonCredentialsPath = "credentials.json";
                GoogleCredential credential;
                using (var stream = new System.IO.FileStream(jsonCredentialsPath, System.IO.FileMode.Open, System.IO.FileAccess.Read))
                {
                    credential = GoogleCredential.FromStream(stream).CreateScoped(Scopes);
                }

                var service = new SheetsService(new BaseClientService.Initializer()
                {
                    HttpClientInitializer = credential,
                    ApplicationName = ApplicationName,
                });
                var config = _basicUtilities.GetConfiguration();

                string spreadsheetId = config.GetSection("EanSpreadSheetID").Value.ToString();
                string SheetName = config.GetSection("EanSheetName").Value.ToString();
                string range = SheetName + "!A2:J";
                int columnIndexToDelete = Convert.ToInt32(config.GetSection("EANcolumnIndexToDelete").Value.ToString());
                string valueToDelete = brandName.Trim();
                SpreadsheetsResource.ValuesResource.GetRequest request1 =
                service.Spreadsheets.Values.Get(spreadsheetId, range);
                ValueRange response1 = await request1.ExecuteAsync();
                IList<IList<Object>> values = response1.Values;
                List<IList<object>> mergedList = new List<IList<object>>();
                if (values != null)
                {
                    var value = values[0];
                    if (value == null || value.Count == 0)
                    {
                        mergedList = data;
                    }
                    else
                    {
                        //if (check)
                        //    values = values.Where(row => !row[columnIndexToDelete].ToString().ToLower().Contains(valueToDelete.ToLower())).ToList();
                        mergedList = values.Concat(data).ToList();
                    }




                }
                else if (values == null)
                {
                    mergedList = data;
                }




                ValueRange valueRange = new ValueRange();
                valueRange.Values = mergedList;
                var updateRequest = service.Spreadsheets.Values.Update(valueRange, spreadsheetId, range);
                updateRequest.ValueInputOption = SpreadsheetsResource.ValuesResource.UpdateRequest.ValueInputOptionEnum.RAW;
                var updateResponse = await updateRequest.ExecuteAsync();




                return true;
            }
            catch (Exception r)
            {
                Console.WriteLine(r);
                return false;
            }
        }


        private string GenderMapping(string gender)
        {
            if (gender == "")
            {
                gender = "Unisex";
            }
            else
            {
                var mappingList = new List<(string, string)>
        {
            ("Unisex","Unisex"),
            ("erwachsene","Unisex"),
            ("herren","Unisex"),
            ("Damen","Damen"),
            ("Unisex Kinder","Kinder"),
            ("Unisex Adults","Unisex"),
            ("Unisex Youth","Kinder"),
            ("Male","Unisex"),
            ("Male Adults","Unisex"),
            ("Female All Ages","Damen"),
            ("Unisex Youth + Adults","Unisex"),
            ("Male Youth","Kinder"),
            ("Female","Damen"),
            ("Female Adults","Damen"),
            ("Unisex All Ages","Unisex"),
            ("Female Youth + Adults","Damen"),
            ("Kinder","Kinder"),
            ("Jungen","Kinder"),
            ("Mädchen","Kinder"),
            ("Female Youth","Kinder"),
            ("Unisex Pre-School","Kinder"),
            ("Unisex Infant","Kinder"),
            ("Male Pre-School","Kinder"),
            ("Male Infant","Kinder"),
            ("Female Pre-School","Kinder")
        };
                var mappingDictionary = new Dictionary<string, string>();
                foreach (var mapping in mappingList)
                {
                    mappingDictionary[mapping.Item1.Trim().ToLower()] = mapping.Item2.Trim().ToLower();
                }
                if (mappingDictionary.ContainsKey(gender.Trim().ToLower()))
                {
                    gender = mappingDictionary[gender.Trim().ToLower()];
                }

                TextInfo textInfo = new CultureInfo("en-US", false).TextInfo;
                gender = textInfo.ToTitleCase(gender);
            }

            return gender;

        }
        [HttpPost]
        public JsonResult GET_DB_DATA()
        {
            DataTable DT_DB_DATA;
            DT_DB_DATA = _dal.GET_DB_DATA(2);
            DataColumnCollection dataColumnCollection = DT_DB_DATA.Columns;
            List<Dictionary<string, object>> _TblBody = _basicUtilities.GetTableRows(DT_DB_DATA);
            List<string> _TblHead = new List<string>();
            for (int j = 0; j < dataColumnCollection.Count; j++)
            {
                string columnName = dataColumnCollection[j].ToString();
                _TblHead.Add(columnName);
            }
            return Json(new { Status = true, Body = _TblBody, Header = _TblHead });
        }

        [HttpPost]
        public JsonResult MAIN_SHEET_DATA_SUMMARY()
        {
            DataTable DT_DB_DATA;
            DT_DB_DATA = _dal.MAIN_SHEET_DATA("1111");
            List<Dictionary<string, object>> _TblBody = _basicUtilities.GetTableRows(DT_DB_DATA);
            return Json(_TblBody);
        }


        //[HttpPost]
        //public JsonResult MAIN_SHEET_DATA(string _BRANDID)
        //{
        //    DataTable DT_DB_DATA;
        //    DT_DB_DATA = _dal.MAIN_SHEET_DATA(_BRANDID);
        //    DataColumnCollection dataColumnCollection = DT_DB_DATA.Columns;
        //    List<Dictionary<string, object>> _TblBody = _basicUtilities.GetTableRows(DT_DB_DATA);
        //    List<string> _TblHead = new List<string>();
        //    for (int j = 0; j < dataColumnCollection.Count; j++)
        //    {
        //        string columnName = dataColumnCollection[j].ToString();
        //        _TblHead.Add(columnName);
        //    }
        //    return Json(new { Status = true, Body = _TblBody, Header = _TblHead });
        //}

        [HttpPost]
        public JsonResult MAIN_SHEET_DATA(string _BRANDID)
        {
            DataTable DT_DB_DATA;
            DT_DB_DATA = _dal.MAIN_SHEET_DATA(_BRANDID);

            List<Dictionary<string, object>> _TblBody = _basicUtilities.GetTableRows(DT_DB_DATA);

            return Json(_TblBody);
        }




        [HttpPost]
        public JsonResult GET_ALT_DATA(string _TYPE)
        {
            DataTable DT_DB_DATA;
            DT_DB_DATA = _dal.GET_ALT_DATA(Convert.ToInt32(_TYPE));

            List<Dictionary<string, object>> _LIST = _basicUtilities.GetTableRows(DT_DB_DATA);

            return Json(_LIST);
        }



        [HttpPost]
        public async Task<bool> PUSH_MAINDB(string _BRAND_NAME, string _BRAND_ID, string _OP)
        {
            DataTable DT_DB_DATA = _dal.GET_DB_DATA(1);
            DataTable newData = DT_DB_DATA;
            newData.Columns.Remove("EAN");
            newData.Columns.Remove("BRANDID");
            bool output = await UploadDataToSheet(newData, _BRAND_NAME, _OP);
            int i = _dal.INSERT_DATA(_OP);

            DataTable EAN_DATA = _dal.GET_EANDB_DATA(_BRAND_ID);

            bool ClearEAN = true;

            DataTable temp_dt = EAN_DATA;
            int fixediteration = 12000;
            int maxcount = temp_dt.Rows.Count;
            int skip = 0;
            bool output2 = false;
            while (skip < maxcount)
            {

                DataTable dt = EAN_DATA.AsEnumerable().Skip(skip).Take(fixediteration).CopyToDataTable();
                skip += fixediteration;
                output2 = await UploadDataToEANSheet(dt, _BRAND_NAME, _OP, ClearEAN);
                ClearEAN = false;
            }
            if (output2)
            {
                int d = _dal.UPDATE_EAN_DATA(_BRAND_ID);
            }
            return output;
        }

        [HttpPost]
        public JsonResult DISCARD_TEMP_DB(int brandID, int type)
        {
            int output = _dal.DISCARD_TEMP_DB(brandID, type);
            return Json(output);
        }

        [HttpPost]
        public JsonResult ADD_SINGLE_ARTICLE(string _BrandID, string _BrandName, string _Price, string _Size, string _Colors, string _Gender
            , string _Ean, string _Article, string _ArticleName)
        {
            try
            {
                int output = _dal.ADD_SINGLE_ARTICLE(_BrandID, _BrandName, _Price, _Size, _Colors, _Gender, _Ean, _Article, _ArticleName);
                return Json(output);
            }
            catch (Exception e)
            {
                Console.WriteLine(e); return Json(0);
            }
        }

        [HttpPost]
        public JsonResult DELETE_ALT_ARTICLE(string _BRAND, string _PROD_ID, string _SIZE)
        {
            try
            {
                _PROD_ID = System.Uri.UnescapeDataString(_PROD_ID);
                _SIZE = System.Uri.UnescapeDataString(_SIZE);
                int output = _dal.DELETE_ALT_ARTICLE(_BRAND, _PROD_ID, _SIZE);
                return Json(output);
            }
            catch (Exception e)
            {
                Console.WriteLine(e); return Json(0);
            }
        }





        public IActionResult Login()
        {
            return View();

        }


        [HttpGet("SheetScript")]
        public async Task<IActionResult> SheetScript(string code)
        {
            try
            {
                var config = _basicUtilities.GetConfiguration();
                var tokenEndpoint = config.GetSection("tokenEndpoint").Value;
                var clientId = config.GetSection("clientId").Value;
                var clientSecret = config.GetSection("clientSecret").Value;
                var redirectUri = config.GetSection("redirectUri").Value;

                var httpClient = new HttpClient();
                var tokenRequest = new Dictionary<string, string>
                {
                    ["code"] = code,
                    ["client_id"] = clientId,
                    ["client_secret"] = clientSecret,
                    ["redirect_uri"] = redirectUri,
                    ["grant_type"] = "authorization_code"
                };

                var tokenResponse = await httpClient.PostAsync(tokenEndpoint, new FormUrlEncodedContent(tokenRequest));
                var tokenContent = await tokenResponse.Content.ReadAsStringAsync();

                dynamic responsevalue = JObject.Parse(tokenContent.ToString());
                string token = responsevalue?.access_token;
                int timeLeft = Convert.ToInt16(responsevalue?.expires_in) / 60;
                SetCookies("AccessToken", tokenContent.ToString(), timeLeft);
                JsonResult jr = await ExecuteCall(token);
                ViewBag.Data = jr.Value;
                return View();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                ViewBag.Data = false;
                return View();
            }
        }



        [HttpGet("Authorize")]
        public async Task<IActionResult> Authorize()
        {
            try
            {
                string token = GetCookie("AccessToken");
                if (token != null && token != "")
                {
                    dynamic responsevalue = JObject.Parse(token.ToString());
                    token = responsevalue?.access_token;
                    JsonResult jr = await ExecuteCall(token);
                    ViewBag.Data = jr.Value;
                    return View("SheetScript");
                }
                else
                {
                    var config = _basicUtilities.GetConfiguration();
                    var clientId = config.GetSection("clientId").Value;
                    var redirectUri = config.GetSection("redirectUri").Value;
                    var scope = config.GetSection("scope").Value;
                    var authEndpoint = config.GetSection("authEndpoint").Value +
                                       $"client_id={clientId}&redirect_uri={redirectUri}&scope={scope}&response_type=code";
                    return Redirect(authEndpoint);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                ViewBag.Data = false;
                return View("SheetScript");
            }

        }


        public async Task<JsonResult> ExecuteCall(string token)
        {
            var config = _basicUtilities.GetConfiguration();
            string Function = config.GetSection("MethodName").Value;
            string ScriptURL = config.GetSection("ScriptURL").Value;
            string EANFunction = config.GetSection("EANMethodName").Value;
            string EANScriptURL = config.GetSection("EANScriptURL").Value;
            try
            {
                var client = new HttpClient();
                var request = new HttpRequestMessage(HttpMethod.Post, ScriptURL);
                request.Headers.Add("Authorization", "Bearer " + token);
                var content = new StringContent("{\r\n  \"function\": \"" + Function + "\"\r\n}", null, "application/json");
                request.Content = content;
                var response = await client.SendAsync(request);
                response.EnsureSuccessStatusCode();
                Console.WriteLine(await response.Content.ReadAsStringAsync());

                // remove duplicates
                //request = new HttpRequestMessage(HttpMethod.Post, EANScriptURL);
                //request.Headers.Add("Authorization", "Bearer " + token);
                //content = new StringContent("{\r\n  \"function\": \"" + EANFunction + "\"\r\n}", null, "application/json");
                //request.Content = content;
                //response = await client.SendAsync(request);
                //response.EnsureSuccessStatusCode();
                //Console.WriteLine(await response.Content.ReadAsStringAsync());





                return Json(true);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                return Json(false);
            }
        }

        #region session cookie delete 
        public void DeleteCookies(string key)
        {
            string Domain = HttpContext.Request.Host.Value.ToString();
            Domain = Domain.Split(':')[0];


            CookieOptions cookieOptions = DynamicCookieOptionForDelete(Domain);
            Response.Cookies.Append(key, "", cookieOptions);

        }

        private CookieOptions DynamicCookieOptionForDelete(string domain)
        {
            return new CookieOptions
            {
                Domain = domain,
                Expires = DateTime.Now.AddDays(-1)
            };
        }

        public void DeleteAllCookie()
        {
            string Domain = HttpContext.Request.Host.Value.ToString();
            Domain = Domain.Split(':')[0];
            foreach (string key in HttpContext.Request.Cookies.Keys)
            {
                HttpContext.Response.Cookies.Append(key, "", DynamicCookieOptionForDelete(Domain));
            }
        }
        public string GetCookie(string key)
        {
            return Request.Cookies[key];
        }
        public void SetCookies(string key, string value, int? expireTime)
        {
            string Domain = HttpContext.Request.Host.Value.ToString();
            Domain = Domain.Split(':')[0];

            DeleteCookies(key);
            CookieOptions option = DynamicCookieOptionForSet(Domain, key, expireTime);
            Response.Cookies.Append(key, value, option);
        }
        private CookieOptions DynamicCookieOptionForSet(string domain, string key, int? expireTime)
        {
            return new CookieOptions
            {
                Domain = domain,
                Expires = key != "RecentArticles" ? DateTime.Now.AddMinutes(expireTime.Value) : DateTime.Now.AddDays(expireTime.Value)
            };
        }
        #endregion













    }
}