using System;
using System.Data;
using System.Data.SqlClient;
using TeamSports.Utilities;
namespace TeamSports.DAL
{
    public class TeamDAL
    {
        public DataTable GET_SORTED_SIZE()
        {
            DataTable dt = new DataTable();
            try
            {
                using (SqlConnection con = new SqlConnection(BasicUtilities.GetConnectionString()))
                {
                    con.Open();
                    SqlCommand cmd = new SqlCommand("GET_SORTED_SIZE", con);
                    cmd.CommandType = CommandType.StoredProcedure;
                    SqlDataAdapter adpt = new SqlDataAdapter(cmd);
                    adpt.Fill(dt);
                }
                return dt;
            }
            finally
            {
                dt.Dispose();
            }
        }
        public DataTable GETDD_DATA(string _type)
        {
            DataTable dt = new DataTable();
            try
            {
                using (SqlConnection con = new SqlConnection(BasicUtilities.GetConnectionString()))
                {
                    con.Open();
                    SqlCommand cmd = new SqlCommand("GETDD_DATA", con);
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.AddWithValue("@TYPE", _type);
                    SqlDataAdapter adpt = new SqlDataAdapter(cmd);
                    adpt.Fill(dt);
                }
                return dt;
            }
            finally
            {
                dt.Dispose();
            }
        }

        public DataTable GET_DB_DATA(int _TYPE)
        {
            DataTable dt = new DataTable();
            try
            {
                using (SqlConnection con = new SqlConnection(BasicUtilities.GetConnectionString()))
                {
                    con.Open();
                    SqlCommand cmd = new SqlCommand("GET_DB_DATA", con);
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.CommandTimeout = 1000;
                    cmd.Parameters.AddWithValue("@TYPE", _TYPE);
                    SqlDataAdapter adpt = new SqlDataAdapter(cmd);
                    adpt.Fill(dt);
                }
                return dt;
            }
            finally
            {
                dt.Dispose();
            }
        }

        public DataTable MAIN_SHEET_DATA(string _BRAND)
        {
            DataTable dt = new DataTable();
            try
            {
                using (SqlConnection con = new SqlConnection(BasicUtilities.GetConnectionString()))
                {
                    con.Open();
                    SqlCommand cmd = new SqlCommand("MAIN_SHEET_DATA", con);
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.CommandTimeout = 1000;
                    cmd.Parameters.AddWithValue("@BRAND", _BRAND);
                    SqlDataAdapter adpt = new SqlDataAdapter(cmd);
                    adpt.Fill(dt);
                }
                return dt;
            }
            finally
            {
                dt.Dispose();
            }
        }

        public DataTable GET_ALT_DATA(int _TYPE)
        {
            DataTable dt = new DataTable();
            try
            {
                using (SqlConnection con = new SqlConnection(BasicUtilities.GetConnectionString()))
                {
                    con.Open();
                    SqlCommand cmd = new SqlCommand("GET_ALT_DATA", con);
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.CommandTimeout = 500;
                    cmd.Parameters.AddWithValue("@TYPE", _TYPE);
                    SqlDataAdapter adpt = new SqlDataAdapter(cmd);
                    adpt.Fill(dt);
                }
                return dt;
            }
            finally
            {
                dt.Dispose();
            }
        }


        public int DISCARD_TEMP_DB(int BrandID)
        {
            int i = 0;
            try
            {
                using (SqlConnection con = new SqlConnection(BasicUtilities.GetConnectionString()))
                {
                    con.Open();
                    SqlCommand cmd = new SqlCommand("DISCARD_TEMP_DB", con);
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.CommandTimeout = 500;
                    cmd.Parameters.AddWithValue("@brandid", BrandID);
                    i = cmd.ExecuteNonQuery();
                }
                return i;
            }
            catch (Exception ex)
            {
                return 0;
            }
        }


        public int INSERT_DATA(string _OP)
        {
            int i = 0;
            try
            {
                using (SqlConnection con = new SqlConnection(BasicUtilities.GetConnectionString()))
                {
                    con.Open();
                    SqlCommand cmd = new SqlCommand("INSERT_DATA", con);
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.AddWithValue("@OP", _OP);
                    i = cmd.ExecuteNonQuery();
                }
                return i;
            }
            catch (Exception ex)
            {
                return 0;
            }
        }






        public int DeleteBrandFiles(string QueryText, string BrandId)
        {
            int i = 0;
            using (SqlConnection con = new SqlConnection(BasicUtilities.GetConnectionString()))
            {
                string subQuery = QueryText== "EAN_DB"? BrandId != "" ? " where brand_id = " + BrandId : "" : BrandId != "" ? " where brandId = " + BrandId : "";

                string Query = "Delete FROM " + QueryText + " " + subQuery;
                con.Open();
                SqlCommand cmd = new SqlCommand(Query, con);
                cmd.CommandType = CommandType.Text;
                cmd.CommandTimeout = 10000;
                i = cmd.ExecuteNonQuery();
            }
            return i;

        }
        public int ADD_SINGLE_ARTICLE(string _BrandID, string _BrandName, string _Price, string _Size, string _Colors, string _Gender, string _Ean, string _Article, string _ArticleName)
        {
            int i = 0;
            try
            {
                using (SqlConnection con = new SqlConnection(BasicUtilities.GetConnectionString()))
                {
                    con.Open();
                    SqlCommand cmd = new SqlCommand("ADD_SINGLE_ARTICLE", con);
                    cmd.Parameters.AddWithValue("@BrandID", _BrandID);
                    cmd.Parameters.AddWithValue("@BrandName", _BrandName);
                    cmd.Parameters.AddWithValue("@Price", _Price);
                    cmd.Parameters.AddWithValue("@Size", _Size);
                    cmd.Parameters.AddWithValue("@Colors", _Colors);
                    cmd.Parameters.AddWithValue("@Gender", _Gender);
                    cmd.Parameters.AddWithValue("@Article", _Article);
                    cmd.Parameters.AddWithValue("@EAN", _Ean);
                    cmd.Parameters.AddWithValue("@ArticleName", _ArticleName);
                    cmd.CommandType = CommandType.StoredProcedure;
                    i = cmd.ExecuteNonQuery();
                }
                return i;
            }
            catch (Exception ex)
            {
                return 0;
            }
        }

        public int DELETE_ALT_ARTICLE(string _BRAND, string _PROD_ID, string SIZE)
        {
            int i = 0;
            try
            {
                using (SqlConnection con = new SqlConnection(BasicUtilities.GetConnectionString()))
                {
                    con.Open();
                    SqlCommand cmd = new SqlCommand("DELETE_ALT_ARTICLE", con);
                    cmd.Parameters.AddWithValue("@BRAND", _BRAND);
                    cmd.Parameters.AddWithValue("@PROD_ID", _PROD_ID);
                    cmd.Parameters.AddWithValue("@SIZE", SIZE);
                    cmd.CommandType = CommandType.StoredProcedure;
                    i = cmd.ExecuteNonQuery();
                }
                return i;
            }
            catch (Exception ex)
            {
                return 0;
            }
        }



    }
}
