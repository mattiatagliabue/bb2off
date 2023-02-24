using System;
using System.Data;
using System.Data.OleDb;
using System.Data.SqlClient;
using ServerPricetagBBFarma.Code.Util;

// Questa classe realizza una connessione al db
// altro commento
namespace ServerPricetagBBFarma.Core.DbHelper
{
    public class DbHelper
    {
        public OleDbConnection cn;
        public SqlConnection cnSQL;
        private DbConnection dbConn;

        public class DbConnection
        {
            public string DbString { get; set; }
            public string DbSqlConnection { get; set; }
        }

        public void ApriDb(DbConnection dbConn)
        {
            string vuota = string.Empty;
            string vuota2 = string.Empty;
            string vuota3 = string.Empty;
            this.dbConn = dbConn;
            ApriDb(true, ref vuota, ref vuota2, ref vuota3);
        }

        private void ApriDb(bool AskSqlConnection, ref string StringaDB, ref string Tempo, ref string Errore)
        {
            DataSet ds = new DataSet();
            TimeSpan ts1 = new TimeSpan(DateTime.Now.Ticks);

            if ((AskSqlConnection == false))
            {
                if ((cn == null))
                {
                    cn = new OleDbConnection(dbConn.DbString);
                    cn.Open();
                }

                if ((cn.State != ConnectionState.Open))
                {
                    cn.Open();
                }

            }
            //if 
            if ((AskSqlConnection == true))
            {
                if ((cnSQL == null))
                {
                    cnSQL = new SqlConnection(dbConn.DbSqlConnection);
                    cnSQL.Open();
                }

                if ((cnSQL.State != ConnectionState.Open))
                {
                    cnSQL.Open();
                }

            }
            
            TimeSpan ts2 = new TimeSpan(DateTime.Now.Ticks);
            Tempo = (ts2 - ts1).ToString();
            return;
        }

        public void ChiudiDb()
        {
            if (cn != null)
            {
                cn.Close();
                cn = null;
            }

            if (cnSQL != null)
            {
                cnSQL.Close();
                cnSQL = null;
            }
        }
    }
}